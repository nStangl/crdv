# Evaluates the correct semantics of nested data types.
# Uses only a single site with a single client.
# Usage: python3 nested.py [-h] [-H HOST] [-p PORT] [-d DATABASE] [-u USER] [-P PASSWORD] [-n NUM_TESTS]

import argparse
import common
import random
import os
import sys

# build query import
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from schema.nested_views import buildQuery


# Markov chain
DATA_TYPES = ['START', 'REGISTER', 'SET', 'MAP', 'LIST', 'COUNTER', 'END']
WEIGHT_MATRIX = [
    [0, 1, 1, 1, 1, 0, 0], # START
    [0, 2, 2, 2, 2, 1, 1], # REGISTER
    [0, 2, 2, 2, 2, 1, 1], # SET
    [0, 2, 2, 2, 2, 1, 1], # MAP
    [0, 2, 2, 2, 2, 1, 1], # LIST
    [0, 0, 0, 0, 0, 0, 1], # COUNTER
    [], # END
]

# Translation between Data types and possible views
VIEWS = {
    'REGISTER': ['RegisterLww'],
    'SET': ['SetAw', 'SetRw', 'SetLww'],
    'MAP': ['MapAwLww', 'MapLww'],
    'LIST': ['List'],
    'COUNTER': ['Counter'],
}

# size of nested structures grows exponentially, so we have to limit the max length
MAX_LENGTH = 8


# Generates a random nested sequence of types, according to the Markov chain above
def genNestedSequence():
    curr = 'START'
    currWeights = WEIGHT_MATRIX[DATA_TYPES.index(curr)]
    types = []
    length = 0

    while curr != 'END' and length < MAX_LENGTH:
        curr = random.choices(DATA_TYPES, currWeights)[0]
        types.append(curr)
        length += 1
        currWeights = WEIGHT_MATRIX[DATA_TYPES.index(curr)]

    if types[length - 1] != 'END':
        types.append('END')

    return types


# Builds a native data structure based on a nested sequence
def buildNestedData(sequence, seq=[0]):
    if sequence[0] == 'END':
        return f'nested-{seq[0]}'

    elif sequence[0] == 'COUNTER':
        seq[0] += 1
        return seq[0]

    elif sequence[0] == 'REGISTER':
        seq[0] += 1
        return buildNestedData(sequence[1:], seq)

    elif sequence[0] == 'LIST':
        collection = []
        for _ in range(2):
            seq[0] += 1
            collection.append(buildNestedData(sequence[1:], seq))
        return collection

    elif sequence[0] == 'SET':
        collection = []
        seq[0] += 1
        # sets will only have one element to simplify the comparison among native and db data, as
        # there is no concept of set in JSON
        collection.append(buildNestedData(sequence[1:], seq))
        return collection

    elif sequence[0] == 'MAP':
        map = {}
        for i in range(2):
            seq[0] += 1
            map[f'k-{i}'] = buildNestedData(sequence[1:], seq)
        return map

    else:
        raise Exception(f'Invalid type: {sequence[0]}')


# Inserts data into the database according to a nested sequence
def insertNestedData(cursor, sequence, id='nested-0', seq=[0]):
    if sequence[0] == 'END':
        return

    if sequence[0] == 'COUNTER':
        seq[0] += 1
        common.counterInc(cursor, id, seq[0])

    elif sequence[0] == 'REGISTER':
        seq[0] += 1
        nextId = f'nested-{seq[0]}'
        common.registerSet(cursor, id, nextId)
        insertNestedData(cursor, sequence[1:], nextId, seq)

    elif sequence[0] == 'SET':
        seq[0] += 1
        nextId = f'nested-{seq[0]}'
        common.setAdd(cursor, id, nextId)
        insertNestedData(cursor, sequence[1:], nextId, seq)

    elif sequence[0] == 'LIST':
        for _ in range(2):
            seq[0] += 1
            nextId = f'nested-{seq[0]}'
            common.listAppend(cursor, id, nextId)
            insertNestedData(cursor, sequence[1:], nextId, seq)

    elif sequence[0] == 'MAP':
        for i in range(2):
            seq[0] += 1
            nextId = f'nested-{seq[0]}'
            common.mapAdd(cursor, id, f'k-{i}', nextId)
            insertNestedData(cursor, sequence[1:], nextId, seq)

    else:
        raise Exception(f'Invalid type: {sequence[0]}')


def testNested():
    conn = common.connection()
    common.initSite(conn)
    cursor = conn.cursor()
    sequence = genNestedSequence()
    print(f'Testing sequence {sequence} ... ', end='')

    # native
    nativeData = buildNestedData(sequence)

    # db
    insertNestedData(cursor, sequence)
    views = [random.choice(VIEWS[x]) for x in sequence[:-1]]
    query = buildQuery(views) + " WHERE id0 = 'nested-0'"
    cursor.execute(query)
    dbData = cursor.fetchone()[1]
    conn.rollback() # do not keep the data between tests to avoid conflicts
    conn.close()

    # compare
    if nativeData != dbData:
        print('Error')
        print(f'\t expected: {nativeData}')
        print(f'\t got: {dbData}')
        return False
    else:
        print('Ok')
        return True


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-H', '--host', type=str, help='Database host', action='store', default='localhost')
    parser.add_argument('-p', '--port', type=str, help='Database port', action='store', default='5432')
    parser.add_argument('-d', '--database', type=str, help='Database name', action='store', default='testdb')
    parser.add_argument('-u', '--user', type=str, help='Username', action='store', default='postgres')
    parser.add_argument('-P', '--password', type=str, help='Password', action='store', default='postgres')
    parser.add_argument('-m', '--mode', type=str, help='Execution mode: sync vs async writes', action='store', default='sync')
    parser.add_argument('-n', type=int, help='Number of tests', action='store', default=100)
    args = parser.parse_args()
    conn = common.connection(args)
    common.prepareTypes(conn)
    common.prepareMode(conn, args.mode)
    conn.close()

    correct = 0
    for _ in range(args.n):
        correct += 1 if testNested() else 0

    print(f'{correct}/{args.n} correct tests')


if __name__ == '__main__':
    main()
