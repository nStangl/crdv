# Evaluates the correct semantics of the implemented data types,
# e.g., list respects order, no duplicate elements in a set, ...
# Uses only a single site with a single client.
# Usage: python3 data-types.py [-h] [-H HOST] [-p PORT] [-d DATABASE] [-u USER] [-P PASSWORD]

from collections import defaultdict
import argparse
import sys
import common
from common import ReadMode
import unittest
import secrets
import random


class Test(unittest.TestCase):
    def testRegister(self):
        conn = common.connection()
        common.initSite(conn)
        cursor = conn.cursor()
        idPrefix = '___test_types_register_'
        id = idPrefix + secrets.token_urlsafe(10)
        value = secrets.token_urlsafe(10)

        # register does not exist yet
        r = common.registerGet(cursor, id, ReadMode.Mvr)
        assert r is None

        # test set and get
        common.registerSet(cursor, id, value)
        r = common.registerGet(cursor, id, ReadMode.Mvr)
        assert len(r) == 1 and r[0] == value
        r = common.registerGet(cursor, id, ReadMode.Lww)
        assert r == value

        # test reset and get
        value = secrets.token_urlsafe(10)
        common.registerSet(cursor, id, value)
        r = common.registerGet(cursor, id, ReadMode.Mvr)
        assert len(r) == 1 and r[0] == value
        r = common.registerGet(cursor, id, ReadMode.Lww)
        assert r == value

        # test multiple registers
        data = {}
        for i in range(100):
            id_ = f'{id}_{i}'
            value_ = f'{value}_{i}'
            common.registerSet(cursor, id_, value_)
            data[id_] = value_
        data = [(k, v) for k, v in data.items()]
        registers = common.registerGetAll(cursor, f'{id}_%', ReadMode.Lww)
        assert sorted(data) == sorted(registers)

        conn.rollback()
        conn.close()


    def testSet(self):
        conn = common.connection()
        common.initSite(conn)
        cursor = conn.cursor()
        idPrefix = '___test_types_set_'
        id = idPrefix + secrets.token_urlsafe(10)
        values = [secrets.token_urlsafe(10) for _ in range(10)]

        # set does not exist yet
        s = common.setGet(cursor, id, ReadMode.Aw)
        assert s is None

        # test add
        common.setAdd(cursor, id, values[0])
        s = common.setGet(cursor, id, ReadMode.Aw)
        contains = common.setContains(cursor, id, values[0], ReadMode.Aw)
        assert contains
        assert s == [values[0]]
        s = common.setGet(cursor, id, ReadMode.Rw)
        assert s == [values[0]]
        s = common.setGet(cursor, id, ReadMode.Lww)
        assert s == [values[0]]

        # test for duplicates
        common.setAdd(cursor, id, values[0])
        s = common.setGet(cursor, id, ReadMode.Aw)
        assert s == [values[0]]

        # test multiple values
        for value in values:
            common.setAdd(cursor, id, value)
        s = common.setGet(cursor, id, ReadMode.Aw)
        assert sorted(s) == sorted(values)

        # test remove
        common.setRmv(cursor, id, values[0])
        contains = common.setContains(cursor, id, values[0], ReadMode.Aw)
        assert not contains
        s = common.setGet(cursor, id, ReadMode.Aw)
        values.pop(0)
        assert sorted(s) == sorted(values)

        # test multiple sets with multiple values
        data = defaultdict(set)
        for i in range(100):
            id_ = f'{id}_{i}'
            values_ = [f'{value}_{i}_{j}' for j, value in enumerate(values)]
            for value in values_:
                common.setAdd(cursor, id_, value)
                data[id_].add(value)
        sets = common.setGetAll(cursor, f'{id}_%', ReadMode.Aw)
        for id_, set_ in sets:
            assert sorted(set_) == sorted(data[id_])

        # test clear
        common.setClear(cursor, id)
        s = common.setGet(cursor, id, ReadMode.Aw)
        assert s is None
        id_ = sets[0][0]
        s = common.setGet(cursor, id_, ReadMode.Aw)
        assert sorted(s) == sorted(data[id_])

        conn.rollback()
        conn.close()


    def testMap(self):
        conn = common.connection()
        common.initSite(conn)
        cursor = conn.cursor()
        idPrefix = '___test_types_map_'
        id = idPrefix + secrets.token_urlsafe(10)
        keys = [secrets.token_urlsafe(10) for _ in range(10)]
        values = [secrets.token_urlsafe(10) for _ in range(10)]
        map_ = {keys[i]: values[i] for i in range(len(keys))}

        # map does not exist yet
        m = common.mapGet(cursor, id, ReadMode.Lww)
        assert m is None

        # add entry
        common.mapAdd(cursor, id, keys[0], values[0])
        m = common.mapGet(cursor, id, ReadMode.Lww)
        assert len(m) == 1 and (m[0].key, m[0].value) == (keys[0], values[0])
        m = common.mapGet(cursor, id, ReadMode.AwMvr)
        assert len(m) == 1 and (m[0].key, m[0].value[0]) == (keys[0], values[0])
        m = common.mapGet(cursor, id, ReadMode.AwLww)
        assert len(m) == 1 and (m[0].key, m[0].value) == (keys[0], values[0])
        m = common.mapGet(cursor, id, ReadMode.RwMvr)
        assert len(m) == 1 and (m[0].key, m[0].value[0]) == (keys[0], values[0])

        # get value by key
        value = common.mapValue(cursor, id, keys[0])
        assert value == values[0]

        # duplicate entry
        common.mapAdd(cursor, id, keys[0], values[0])
        m = common.mapGet(cursor, id, ReadMode.Lww)
        assert len(m) == 1 and (m[0].key, m[0].value) == (keys[0], values[0])

        # change value
        value = secrets.token_urlsafe(10)
        common.mapAdd(cursor, id, keys[0], value)
        m = common.mapGet(cursor, id, ReadMode.Lww)
        assert len(m) == 1 and (m[0].key, m[0].value) == (keys[0], value)

        # multiple entries
        for i in range(len(map_)):
            common.mapAdd(cursor, id, keys[i], values[i])
        m = common.mapGet(cursor, id, ReadMode.Lww)
        assert len(m) == len(map_)
        for entry in m:
            assert entry.value == map_[entry.key]

        # remove entry
        del map_[keys[0]]
        common.mapRmv(cursor, id, keys[0])
        contains = common.mapContains(cursor, id, keys[0], ReadMode.Lww)
        assert not contains
        m = common.mapGet(cursor, id, ReadMode.Lww)
        assert len(m) == len(map_)
        for entry in m:
            assert entry.value == map_[entry.key]

        # multiple maps
        data = defaultdict(dict)
        for i in range(100):
            id_ = f'{id}_{i}'
            keys_ = [f'{key}_{i}_{j}' for j, key in enumerate(keys)]
            values_ = [f'{value}_{i}_{j}' for j, value in enumerate(values)]
            for i in range(len(keys_)):
                common.mapAdd(cursor, id_, keys_[i], values_[i])
                data[id_][keys_[i]] = values_[i]
        maps = common.mapGetAll(cursor, f'{id}_%', ReadMode.Lww)
        for id_, map_ in maps:
            for key, value in map_:
                assert value == data[id_][key]

        # test clear
        common.mapClear(cursor, id)
        m = common.mapGet(cursor, id, ReadMode.AwMvr)
        assert m is None
        id_ = next(iter(data))
        m = common.mapGet(cursor, id_, ReadMode.Lww)
        for key, value in m:
            assert value == data[id_][key]

        conn.rollback()
        conn.close()


    def testCounter(self):
        conn = common.connection()
        common.initSite(conn)
        cursor = conn.cursor()
        idPrefix = '___test_types_counter_'
        id = idPrefix + secrets.token_urlsafe(10)
        value = random.randint(0, 10)

        # counter does not yet exist
        c = common.counterGet(cursor, id)
        assert c is None

        # init counter
        common.counterInc(cursor, id, value)
        c = common.counterGet(cursor, id)
        assert c == value

        # test inc
        delta = random.randint(1, 10)
        value += delta
        common.counterInc(cursor, id, delta)
        c = common.counterGet(cursor, id)
        assert c == value

        # test dec
        delta = random.randint(1, 10)
        value -= delta
        common.counterDec(cursor, id, delta)
        c = common.counterGet(cursor, id)
        assert c == value

        # multiple counters
        data = []
        for i in range(100):
            id_ = f'{id}_{i}'
            value = random.randint(1, 10)
            data.append((id_, value))
            common.counterInc(cursor, id_, value)
        counters = common.counterGetAll(cursor, f'{id}_%')
        assert sorted(counters) == sorted(data)

        conn.rollback()
        conn.close()


    def testList(self):
        conn = common.connection()
        common.initSite(conn)
        cursor = conn.cursor()
        idPrefix = '___test_types_list_'
        id = idPrefix + secrets.token_urlsafe(10)
        values = [secrets.token_urlsafe(10) for _ in range(10)]

        # list does not exist yet
        l = common.listGet(cursor, id)
        assert l is None

        # test append elements one by one
        for v in values:
            common.listAppend(cursor, id, v)
        l = common.listGet(cursor, id)
        assert l == values

        # test insert at random positions
        for _ in range(100):
            value = secrets.token_urlsafe(10)
            index = random.randint(0, len(values) - 1)
            values.insert(index, value)
            common.listAdd(cursor, id, index, value)
        l = common.listGet(cursor, id)
        assert l == values

        # remove random elements
        for _ in range(10):
            index = random.randint(0, len(values) - 1)
            values.pop(index)
            common.listRmv(cursor, id, index)
        l = common.listGet(cursor, id)
        assert l == values

        # lists of lists
        data = {}
        for i in range(10):
            id_ = f'{id}_{i}'
            values_ = [f'{value}_{i}_{j}' for j, value in enumerate(values)]
            data[id_] = values_
            for value in values_:
                common.listAppend(cursor, id_, value)
        lists = common.listGetAll(cursor, f'{id}_%')
        for id_, list in lists:
            assert list == data[id_]

        # test clear
        common.listClear(cursor, id)
        l = common.listGet(cursor, id)
        assert l is None
        id_ = next(iter(data))
        l = common.listGet(cursor, id_)
        assert l == data[id_]

        conn.rollback()
        conn.close()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-H', '--host', type=str, help='Database host', action='store', default='localhost')
    parser.add_argument('-p', '--port', type=str, help='Database port', action='store', default='5432')
    parser.add_argument('-d', '--database', type=str, help='Database name', action='store', default='testdb')
    parser.add_argument('-u', '--user', type=str, help='Username', action='store', default='postgres')
    parser.add_argument('-P', '--password', type=str, help='Password', action='store', default='postgres')
    parser.add_argument('-m', '--mode', type=str, help='Execution mode: sync vs async writes', action='store', default='sync')
    args, other = parser.parse_known_args()
    conn = common.connection(args)
    common.prepareTypes(conn)
    common.prepareMode(conn, args.mode)
    conn.close()
    unittest.main(argv=[sys.argv[0]] + other)


if __name__ == '__main__':
    main()
