# Updates the databases' connections in various configurations files.
# Receives a YAML file with the connections and the files to update for each engine.

import sys
import ruamel.yaml
import re
import os

yaml = ruamel.yaml.YAML()
yaml.width = 1000000000

def updateClusterFile(clusterFile, connections):
    if os.path.exists(clusterFile):
        print(f'  Updating {clusterFile}')
        clusterConnections = []
        cluster = {'connections': clusterConnections}

        for connection in connections:
            clusterConnections.append({
                'connection': {k:v for k,v in connection.items() if k != 'targetDBs'},
                'targetDBs': connection['targetDBs'] if 'targetDBs' in connection else [connection['dbname']]
            })

        with open(clusterFile, 'w') as f:
            yaml.dump(cluster, f)
    else:
        print(f'  Warning: file {clusterFile} does not exist, skipping.')


def updateBenchmark(configFiles, formattedConnections):
    for configFile in configFiles:
        if os.path.exists(configFile):
            print(f'  Updating {configFile}')
            with open(configFile) as f:
                config = yaml.load(f)
                config['connection'] = formattedConnections
            with open(configFile, 'w') as f:
                yaml.dump(config, f)
        else:
            print(f'  Warning: file {configFile} does not exist, skipping.')


def updatePostgresBenchmark(configFiles, connections):
    formattedConnections = []
    for c in connections:
        if 'targetDBs' in c:
            formattedConnections.extend([
                f"host={c['host']} port={c['port']} dbname={t} user={c['user']} password={c['password']} sslmode=disable"
                for t in c['targetDBs']
            ])
        else:
            formattedConnections.append(
                f"host={c['host']} port={c['port']} dbname={c['dbname']} user={c['user']} password={c['password']} sslmode=disable"
            )
    updateBenchmark(configFiles, formattedConnections)


def updateRiakBenchmark(configFiles, connections):
    formattedConnections = [
        f"{c['host']}:{c['port']}"
        for c in connections
    ]
    updateBenchmark(configFiles, formattedConnections)


def updateTestScripts(files, info):
    ips = {
        'crdv': info['crdv']['connections'],
        'pg_crdt': info['pg_crdt']['connections'],
        'riak': info['riak']['connections'],
    }
    
    # network
    if os.path.exists(files['network']):
        print(f"  Updating {files['network']}")

        with open(files['network']) as f:
            network = f.read()

        network = re.sub(r'METRICS_SERVER_CRDV=".*?:', f'METRICS_SERVER_CRDV="{ips["crdv"][0]["host"]}:', network)
        network = re.sub(r'METRICS_SERVER_PG_CRDT=".*?:', f'METRICS_SERVER_PG_CRDT="{ips["pg_crdt"][0]["host"]}:', network)
        network = re.sub(r'METRICS_SERVER_RIAK=".*?:', f'METRICS_SERVER_RIAK="{ips["riak"][0]["host"]}:', network)

        with open(files['network'], 'wb') as f:
            f.write(network.encode())
    else:
        print(f"  Warning: file {files['network']} does not exist, skipping")

    # delay
    if os.path.exists(files['delay']):
        print(f"  Updating {files['delay']}")

        with open(files['delay']) as f:
            delay = f.read()

        delay = re.sub(r'PARTITION_NETWORK_SERVER_CRDV=".*?:', f'PARTITION_NETWORK_SERVER_CRDV="{ips["crdv"][0]["host"]}:', delay)
        blockIps = '&'.join(["ip=" + c["host"] for c in ips["crdv"][1:]])
        delay = re.sub(r'BLOCK_IPS_CRDV=".*?"', f'BLOCK_IPS_CRDV="{blockIps}"', delay)
        delay = re.sub(r'PARTITION_NETWORK_SERVER_PG_CRDT=".*?:', f'PARTITION_NETWORK_SERVER_PG_CRDT="{ips["pg_crdt"][0]["host"]}:', delay)
        delay = re.sub(r'PARTITION_NETWORK_SERVER_RIAK=".*?:', f'PARTITION_NETWORK_SERVER_RIAK="{ips["riak"][0]["host"]}:', delay)
        blockIps = '&'.join(["ip=" + c["host"] for c in ips["riak"][1:]])
        delay = re.sub(r'BLOCK_IPS_RIAK=".*?"', f'BLOCK_IPS_RIAK="{blockIps}"', delay)

        with open(files['delay'], 'wb') as f:
            f.write(delay.encode())
    else:
        print(f"  Warning: file {files['delay']} does not exist, skipping")


if len(sys.argv) < 2:
    exit("Missing connections' info file.")

with open(sys.argv[1]) as f:
    info = yaml.load(f)


for key, value in info.items():
    print(f'Updating {key}')

    if key == 'crdv':
        updateClusterFile(value['clusterFile'], value['connections'])

    if key in ('crdv', 'native', 'pg_crdt', 'electric'):
        updatePostgresBenchmark(value['benchmarkConfigs'], value['connections'])
    elif key == 'riak':
        updateRiakBenchmark(value['benchmarkConfigs'], value['connections'])
    elif key == 'test_scripts':
        updateTestScripts(value['files'], info)
    else:
        print(f'Warning: Invalid config ({key})')
