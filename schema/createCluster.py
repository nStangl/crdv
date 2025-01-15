# Creates a cluster according to the information in the provided file (see cluster.yml).
# Note: non-existing targetDBs will be created, while existing ones will be destroyed.
# Provided connections must be able to drop and create databases, or the targetDB must be the same
# as the one in the connection.

import psycopg2
import yaml
import sys
from createSchema import createSchema
import re

def connectionInfoStr(info):
    return f"{info['host']}:{info['port']}/{info['dbname']}"

if len(sys.argv) < 2:
    exit('Missing cluster YAML file')

# ring replication
if len(sys.argv) >= 3 and sys.argv[2] == 'ring':
    ring = True
else:
    ring = False

with open(sys.argv[1]) as f:
    clusterInfo = yaml.load(f, Loader=yaml.Loader)

sites= {} # id -> connection, connection info

# create all connections and databases first
i = 1
for entry in clusterInfo['connections']:
    print(f"Connecting to {connectionInfoStr(entry['connection'])}")
    conn = psycopg2.connect(**entry['connection'])
    conn.autocommit = True
    cursor = conn.cursor()

    for target in entry['targetDBs']:
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {target}")
            cursor.execute(f"CREATE DATABASE {target}")
        except:
            pass

        targetConnInfo = dict(entry['connection'])
        targetConnInfo['dbname'] = target

        print(f"Creating schema for site {connectionInfoStr(targetConnInfo)}")
        createSchema(**targetConnInfo, quiet=True)

        connTarget = psycopg2.connect(**targetConnInfo)
        connTarget.autocommit = True
        sites[i] = (connTarget, targetConnInfo)
        i += 1

# init the sites
for id, (conn, _) in sites.items():
    print(f'Initializing Site {id}')
    cursor = conn.cursor()
    cursor.execute('SELECT initSite(%s)', (id,))
    cursor.close()

# connect each site to the others
for id, (conn, _) in sites.items():
    cursor = conn.cursor()


    for otherId, (_, otherInfo) in sites.items():
        if id == otherId:
            continue

        # ring assumes a circular replication: 0->1->2->...->0
        if ring and ((otherId != len(sites) and otherId != id - 1) or (otherId == len(sites) and id != 1)):
            replicate = False
        else:
            replicate = True

        print(f'Adding Site {otherId} to {id}')
        cursor.execute('SELECT addRemoteSite(%s, %s, %s, %s, %s, %s, %s)',
                       (otherId, otherInfo['host'], str(otherInfo['port']), otherInfo['dbname'],
                        otherInfo['user'], otherInfo['password'], replicate))

        # if ring, also update the publication to send everything except the next site's own data
        if ring:
            nextId = ((id + 1) % (len(sites) + 1))
            if nextId == 0:
                nextId = 1
            cursor.execute('ALTER PUBLICATION shared_pub SET TABLE Shared WHERE (site <> %s);', (nextId,))

print('Done')
print('\nCluster info')
for id, (_, info) in sites.items():
    print(f'  Site {id}: {connectionInfoStr(info)}')
