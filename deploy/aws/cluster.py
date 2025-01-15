# Deploys a cluster with all databases and benchmarks.
# Use cluster.py -h for help.

import argparse
import subprocess
import json
import time
import asyncio

REGION = "us-east-2"


def exec(command):
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        exit(f"Error when executing {' '.join(command)}: {result.stdout + result.stderr}")
    return result.stdout


def exec_watch(command):
    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(process.stdout.readline, ""):
        print(stdout_line, end='')
    process.stdout.close()


async def exec_async(command, stdoutFilename=''):
    if stdoutFilename != '':
        with open(stdoutFilename, 'w') as f:
            process = await asyncio.create_subprocess_exec(*command, stdout=f, stderr=f)
    else:
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    await process.communicate()
    return await process.wait()


def clusterInfo():
    result = exec([
        "aws",
        "ec2",
        "describe-instances",
        "--region", REGION,
        "--query", "Reservations[*].Instances[?State.Name==`running`].{Name: Tags[?Key==`Name`].Value | [0], InstanceId: InstanceId, PrivateIpAddress: PrivateIpAddress, PublicIpAddress: PublicIpAddress}"
    ])

    info = {}
    for sites in json.loads(result):
        for site in sites:
            info[site['Name'] + "-" + site['PublicIpAddress']] = site

    return info


def deploy(nServers):
    cluster = clusterInfo()
    if 'client' in cluster:
        exit(f'Cluster already deployed: {json.dumps(cluster, indent=4)}')

    # client
    exec(["aws", "--region", REGION, "ec2", "run-instances", "--cli-input-json", "file://client.json"])

    # servers
    exec(["aws", "--region", REGION, "ec2", "run-instances", "--cli-input-json", "file://server.json", "--count", str(nServers)])

    time.sleep(5)
    print('Cluster deployed')
    print(json.dumps(clusterInfo(), indent=4))


async def prepare(codePath, install, ring):
    cluster = clusterInfo()
    coroutines = []

    # copy code
    for name, site in cluster.items():
        if name.startswith('client') or name.startswith('server'):
            print(f'Copying {codePath} ({name})')
            p = exec_async(["scp", "-o", "StrictHostKeyChecking=no", codePath, f"ubuntu@{site['PublicIpAddress']}:sql-crdt.zip"])
            coroutines.append(p)

    await asyncio.gather(*coroutines)
    coroutines.clear()

    # extract
    for name, site in cluster.items():
        if name.startswith('client') or name.startswith('server'):
            print(f'Extracting ({name})')
            command = f'''
                sudo apt update;
                sudo DEBIAN_FRONTEND=noninteractive apt install -y zip unzip zip;
                sudo DEBIAN_FRONTEND=noninteractive apt remove unattended-upgrades -y;
                unzip -o sql-crdt.zip; cd sql-crdt/benchmarks; sudo chmod +x *.sh; cd ../deploy/; sudo chmod +x *.sh
            '''
            p = exec_async(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command],  name + '.txt')
            coroutines.append(p)

    await asyncio.gather(*coroutines)
    coroutines.clear()

    # install databases, python, go
    if install:
        for name, site in cluster.items():
            if not (name.startswith('client') or name.startswith('server')):
                continue

            if name.startswith('client'):
                print(f'Preparing client (saving output to {name}.txt)')
                command = f'''
                    cd sql-crdt/deploy; sudo chmod +x *.sh;
                    ./install_python.sh; ./install_go.sh; source $HOME/.profile;
                    cd ../benchmarks; sudo chmod +x *.sh;
                '''
            else:
                print(f'Preparing server (saving output to {name}.txt)')
                command = f'''
                    cd sql-crdt/deploy; sudo chmod +x *.sh;
                    ./install_postgres.sh; ./install_extensions.sh;
                    ./install_electric.sh;
                    ./install_riak.sh;
                    ./install_pg_crdt.sh;
                    ./install_metrics_server.sh;
                    ./install_network_partition_server.sh;
                '''

            p = exec_async(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command], f"{name}.txt")
            coroutines.append(p)

        print('Should take around 10 mins')
        await asyncio.gather(*coroutines)
        coroutines.clear()

    # start the databases in the servers
    i = 1
    for name, site in sorted(cluster.items()):
        if site["Name"] == 'server':
            print(f'Starting databases at {name}')
            command = f'''
                cd sql-crdt/deploy;
                #cd electric; ./run.sh; cd ..;
                cd riak; ./run.sh 1; ./create-bucket-types.sh 1
            '''
            exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command])
            print()
            i += 1

    time.sleep(5)

    # prepare the riak cluster
    """
    print('Preparing the riak cluster')
    riakClusterInfo = {}
    i = 1
    for name, site in sorted(cluster.items()):
        if site["Name"] == 'server':
            command = f'''
                cd sql-crdt/deploy/riak;
                ./repl-prepare.sh C{i}
            '''
            riakClusterInfo[f'C{i}'] = site["PrivateIpAddress"]
            exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command])
            i += 1

    i = 1
    for name, site in sorted(cluster.items()):
        if site["Name"] == 'server':
            if ring:
                nextClusterId = (i+1) % (len(riakClusterInfo) + 1)
                if nextClusterId == 0:
                    nextClusterId = 1

            for clusterName, hostnameToConnect in riakClusterInfo.items():
                if hostnameToConnect != site["PrivateIpAddress"]:
                    if ring and clusterName != f'C{nextClusterId}':
                        continue
                    command = f'''
                        cd sql-crdt/deploy/riak;
                        ./repl-connect.sh {clusterName} {hostnameToConnect}
                    '''
                    exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command])
            i += 1
    """
    # prepare client connections; create crdv cluster
    print('Updating client config files and creating crdv cluster')
    client = [x for x in cluster.values() if x['Name'] == 'client'][0]
    serverIps = ' '.join([x['PrivateIpAddress'] for x in cluster.values() if x['Name'] == 'server'])
    command = f'''
        cd sql-crdt/deploy;
        python3 update_connections.py connections.yaml --all {serverIps};
        echo "Connections used:";
        cat connections.yaml;
        python3 update_configs.py connections.yaml;
        cd ../schema;
        python3 createCluster.py cluster.yaml {"ring" if ring else ""}
    '''
    exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{client['PublicIpAddress']}", command])

    print('Prepare done')
    
    
def cli():
    cluster = clusterInfo()
    client = [x for x in cluster.values() if x['Name'] == 'client'][0]
    print(f"ssh -o StrictHostKeyChecking=no ubuntu@{client['PublicIpAddress']} -t 'cd sql-crdt/benchmarks; bash --login'")


def results():
    cluster = clusterInfo()
    client = [x for x in cluster.values() if x['Name'] == 'client'][0]
    zipCommand = f'''
        cd sql-crdt/benchmarks
        zip -r results.zip results
    '''
    exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{client['PublicIpAddress']}", zipCommand])
    exec_watch(["scp", "-o", "StrictHostKeyChecking=no", f"ubuntu@{client['PublicIpAddress']}:sql-crdt/benchmarks/results.zip", "."])


def terminate():
    cluster = clusterInfo()
    ids = [x["InstanceId"] for x in cluster.values() if x["Name"] in ('client', 'server')]
    exec(["aws", "--region", REGION, "ec2", "terminate-instances", "--instance-ids"] + ids)
    print('Cluster terminated')


def truncate():
    cluster = clusterInfo()
    
    for name, site in sorted(cluster.items()):
        if site["Name"] == 'server':
            print(name)
            command = f'''
                psql -U postgres -d testdb -c "delete from shared"
            '''
            exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command])


def runningQueries():
    cluster = clusterInfo()
    
    for name, site in sorted(cluster.items()):
        print(site["PublicIpAddress"])
        if site["Name"] == 'server':
            command = f'''
                psql -U postgres -d testdb -c "SELECT datname, pid, state, left(query, 50) FROM pg_stat_activity WHERE state <> 'idle' AND query NOT LIKE '% FROM pg_stat_activity %';"
            '''
            exec_watch(["ssh", "-o", "StrictHostKeyChecking=no", f"ubuntu@{site['PublicIpAddress']}", command])



def main():
    parser = argparse.ArgumentParser(description='Deploy a cluster')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute', required=True)
    parserDeploy = subparsers.add_parser('deploy', help='Deploy the instances')
    parserDeploy.add_argument('servers', type=int, help='Number of servers to deploy')
    parserStatus = subparsers.add_parser('status', help='Cluster status')
    parserPrepare = subparsers.add_parser('prepare', help='Prepare the instances')
    parserPrepare.add_argument('code', type=str, help='Path of the .zip with the code')
    parserPrepare.add_argument('-i', '--install', help='Install the databases, Python and Go. Otherwise, skip.', action='store_true')
    parserPrepare.add_argument('-r', '--ring', help='Deploy CRDV and Riak using a ring replication architecture.', action='store_true')
    parserCli = subparsers.add_parser('cli', help='Prints a command to login to the client')
    parserResults = subparsers.add_parser('results', help='Copies the results to the host')
    parserTerminate = subparsers.add_parser('terminate', help='Terminate the instances')
    parserTruncate = subparsers.add_parser('truncate', help='Truncate the shared tables')
    parserRunningQueries = subparsers.add_parser('queries', help='Truncate the shared tables')
    args = parser.parse_args()

    if args.command == 'deploy':
        deploy(args.servers)
    elif args.command == 'prepare':
        asyncio.run(prepare(args.code, args.install, args.ring))
    elif args.command == 'status':
        print(json.dumps(clusterInfo(), indent=4))
    elif args.command == 'cli':
        cli()
    elif args.command == 'results':
        results()
    elif args.command == 'terminate':
        terminate()
    elif args.command == 'truncate':
        truncate()
    elif args.command == 'queries':
        runningQueries()


if __name__ == '__main__':
    main()
