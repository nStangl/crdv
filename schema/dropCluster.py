# Clears a cluster according to the information in the provided file (see cluster.yml).
# Provided connections must be able to drop databases.

import psycopg2
import yaml
import sys
from createSchema import dropSchema
import time


def connect(info):
    conn = psycopg2.connect(**info)
    conn.autocommit = True
    cursor = conn.cursor()
    return conn, cursor


def connectionInfoStr(info):
    return f"{info['host']}:{info['port']}/{info['dbname']}"


if len(sys.argv) < 2:
    exit('Missing cluster YAML file')

with open(sys.argv[1]) as f:
    clusterInfo = yaml.load(f, Loader=yaml.Loader)

sites= {} # id -> connection, connection info

# drop the schemas
for entry in clusterInfo['connections']:
    print(f"Connecting to {connectionInfoStr(entry['connection'])}")
    conn, cursor = connect(entry['connection'])

    for target in entry['targetDBs']:
        targetConnInfo = dict(entry['connection'])
        targetConnInfo['dbname'] = target
        print(f'Destroying schema for {connectionInfoStr(targetConnInfo)}')
        dropSchema(**targetConnInfo)

time.sleep(1)

# drop the databases
for entry in clusterInfo['connections']:
    print(f"Connecting to {connectionInfoStr(entry['connection'])}")
    conn, cursor = connect(entry['connection'])

    for target in entry['targetDBs']:
        if target != entry['connection']['dbname']:
            targetConnInfo = dict(entry['connection'])
            targetConnInfo['dbname'] = target
            print(f'Dropping {connectionInfoStr(targetConnInfo)}')
            cursor.execute(f"DROP DATABASE IF EXISTS {target}")
        else:
            print(f'Target database is the same as the connection database ({target}), skipping drop.')

print('Done')
