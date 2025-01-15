# Updates the connections in the connections YAML file.
# Receives the YAML file and the list of ips.

import argparse
from typing import Dict, List
import ruamel.yaml

yaml = ruamel.yaml.YAML()
yaml.width = 1000000000

def update(filename: str, ips: Dict[str, List[str]], scale: int, sourceDB: str):
    with open(filename) as f:
        cluster = yaml.load(f)

    if scale == 1:
        c = cluster['crdv']['connections'][0]
        if 'targetDBs' in c:
            del c['targetDBs']
        cluster['crdv']['connections'] = [cluster['crdv']['connections'][0] | {"host": ip} for ip in ips['crdv']]
    else:
        cluster['crdv']['connections'] = [cluster['crdv']['connections'][0]]
        cluster['crdv']['connections'][0]['dbname'] = sourceDB
        cluster['crdv']['connections'][0]['targetDBs'] = [f'testdb{i}' for i in range(1, scale + 1)]

    cluster['native']['connections'] = [cluster['native']['connections'][0] | {"host": ips['native'][0]}]
    cluster['pg_crdt']['connections'] = [cluster['pg_crdt']['connections'][0] | {"host": ips['pg_crdt'][0]}]
    cluster['electric']['connections'] = [cluster['electric']['connections'][0] | {"host": ips['electric'][0]}]
    cluster['riak']['connections'] = [cluster['riak']['connections'][0] | {"host": ip} for ip in ips['riak']]

    with open(filename, 'w') as f:
        yaml.dump(cluster, f)


def main():
    parser = argparse.ArgumentParser(description='Update the YAML file with the various connections')
    parser.add_argument('file', type=str, help='Connections YAML file')
    parser.add_argument('-s', '--scale', type=int,
                        help='Number of CRDV databases in the same server (single server multiple sites deployments)',
                        default=1)
    parser.add_argument('-d', '--sourceDB', type=str, help='Source db when scale > 1 (to create the other databases)',
                        default='testdb')
    parser.add_argument('--all', type=str, nargs='+', help='List of hosts to apply to all systems', required=False)
    parser.add_argument('--crdv', type=str, nargs='+', help='List of hosts to apply to CRDV', required=False)
    parser.add_argument('--native', type=str, nargs=1, help='Host to apply to native', required=False)
    parser.add_argument('--pg_crdt', type=str, nargs=1, help='Host to apply to Pg_crdt', required=False)
    parser.add_argument('--electric', type=str, nargs=1, help='Host to apply to ElectricSQL', required=False)
    parser.add_argument('--riak', type=str, nargs='+', help='List of hosts to apply to Riak', required=False)
    args = parser.parse_args()

    ips = {}
    if args.all:
        ips['crdv'] = args.all
        ips['native'] = args.all
        ips['pg_crdt'] = args.all
        ips['electric'] = args.all
        ips['riak'] = args.all
    if args.crdv:
        ips['crdv'] = args.crdv
    if args.native:
        ips['native'] = args.native
    if args.pg_crdt:
        ips['pg_crdt'] = args.pg_crdt
    if args.electric:
        ips['electric'] = args.electric
    if args.riak:
        ips['riak'] = args.riak

    if 'crdv' not in ips:
        exit('Missing crdv hosts.')
    if 'native' not in ips:
        exit('Missing native hosts.')
    if 'pg_crdt' not in ips:
        exit('Missing pg_crdt hosts.')
    if 'electric' not in ips:
        exit('Missing electric hosts.')
    if 'riak' not in ips:
        exit('Missing riak hosts.')

    update(args.file, ips, args.scale, args.sourceDB)


if __name__ == '__main__':
    main()
