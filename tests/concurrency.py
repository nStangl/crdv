import argparse
import sys
from typing import List
import common
import unittest
import secrets
from time import sleep
import random


class Site:
    def __init__(self, host, port, dbname, user, password, mode) -> None:
        self.info = (host, port, dbname, user, password)
        self.conn = common.connect(host, port, dbname, user, password)
        common.prepareTypes(self.conn)
        common.prepareMode(self.conn, mode)
        self.mode = mode
        self.cursor = self.conn.cursor()


def initSites(sites):
    for i, site in enumerate(sites):
        cursor = site.cursor
        try:
            cursor.execute('SELECT initSite(%s)', (i + 1, ))
            site.conn.commit()
        except:
            site.conn.rollback()

    for i, site in enumerate(sites):
        cursor = site.cursor
        for j, remoteSite in enumerate(sites):
            if i != j:
                try:
                    cursor.execute('SELECT addRemoteSite(%s, %s, %s, %s, %s, %s)', (j + 1, *remoteSite.info))
                    site.conn.commit()
                except:
                    site.conn.rollback()

    # wait until the replication slots become active
    for site in sites:
        cursor = site.cursor
        done = False
        while not done:
            cursor.execute('SELECT active FROM pg_replication_slots')
            if all([x[0] for x in cursor.fetchall()]):
                done = True
            else:
                sleep(1)

    # disable auto merge
    for site in sites:
        cursor = site.cursor
        cursor.execute('SELECT unschedule_merge_daemon()')


def replicateAndMergeAllSites(sites: List[Site]):
    for s in sites:
        s.conn.commit()
        common.replicate(s.cursor)
    for s in sites:
        if s.mode == 'sync':
            common.merge(s.cursor)
            s.conn.commit()


def compareAllSites(sites, getFunction, key, readMode, valueToCompare, preprocessReads=lambda x: x):
    valuesRead = []
    for s in sites:
        if readMode is not None:
            valuesRead.append(getFunction(s.cursor, key, readMode))
        else:
            valuesRead.append(getFunction(s.cursor, key))

    if not all([preprocessReads(v) == valueToCompare for v in valuesRead]):
        print(valuesRead)
        print(valueToCompare)
    assert all([preprocessReads(v) == valueToCompare for v in valuesRead])


class Test(unittest.TestCase):
    sites: List[Site] = []

    def testRegister(self):
        initSites(self.sites)
        id = '___test_concurrency_register_' + secrets.token_urlsafe(10)
        values = [secrets.token_urlsafe(10) for _ in range(len(self.sites))]
        valueFinal = secrets.token_urlsafe(10)

        # multiple sites setting the same register
        for i, s in enumerate(self.sites):
            common.registerSet(s.cursor, id, values[i])
            sleep(0.01) # to ensure the correct order using the physical time

        # reads before replication
        for i, s in enumerate(self.sites):
            assert common.registerGet(s.cursor, id, common.ReadMode.Mvr) == [values[i]]

        # merge data
        replicateAndMergeAllSites(self.sites)

        for s in self.sites:
            s.conn.commit()

        # confirm all sites read the same values after merging, as they are concurrent
        compareAllSites(self.sites, common.registerGet, id, common.ReadMode.Mvr, values)
        compareAllSites(self.sites, common.registerGet, id, common.ReadMode.Lww, values[-1])

        # new write to replace the existing ones
        common.registerSet(self.sites[0].cursor, id, valueFinal)
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.registerGet, id, common.ReadMode.Mvr, [valueFinal])


    def testSet(self):
        initSites(self.sites)
        id = '___test_concurrency_set_' + secrets.token_urlsafe(10)
        values = [[secrets.token_urlsafe(10) for _ in range(10)] for _ in range(len(self.sites))]

        # add multiple values in multiple sites
        for i, s in enumerate(self.sites):
            for v in values[i]:
                common.setAdd(s.cursor, id, v)

        replicateAndMergeAllSites(self.sites)
        allValues = sorted(set([x for l in values for x in l]))
        compareAllSites(self.sites, common.setGet, id, common.ReadMode.Aw, allValues, preprocessReads=sorted)

        # test concurrent add of the same value (check for duplicates)
        value = secrets.token_urlsafe(10)
        common.setAdd(self.sites[0].cursor, id, value)
        common.setAdd(self.sites[1].cursor, id, value)
        self.sites[0].conn.commit()
        self.sites[1].conn.commit()
        replicateAndMergeAllSites(self.sites)
        for i, s in enumerate(self.sites):
            self.sites[i].conn.commit()
        allValues = set([x for l in values for x in l])
        allValues.add(value)
        allValues = sorted(allValues)
        compareAllSites(self.sites, common.setGet, id, common.ReadMode.Aw, allValues, preprocessReads=sorted)

        # test concurrent add and remove of the same value
        value = secrets.token_urlsafe(10)
        common.setAdd(self.sites[0].cursor, id, value)
        sleep(0.01) # to ensure that rmv happens after add using the physical time
        common.setRmv(self.sites[1].cursor, id, value)
        replicateAndMergeAllSites(self.sites)
        allValuesAw = [x for x in allValues]
        allValuesAw.append(value)
        allValuesAw = sorted(allValuesAw)
        compareAllSites(self.sites, common.setGet, id, common.ReadMode.Aw, allValuesAw, preprocessReads=sorted)
        compareAllSites(self.sites, common.setGet, id, common.ReadMode.Rw, allValues, preprocessReads=sorted)
        compareAllSites(self.sites, common.setGet, id, common.ReadMode.Lww, allValues, preprocessReads=sorted)


    def testMap(self):
        initSites(self.sites)
        id = '___test_concurrency_map_' + secrets.token_urlsafe(10)
        entries = [[(secrets.token_urlsafe(10), secrets.token_urlsafe(10))  for _ in range(10)] for _ in range(len(self.sites))]

        # add multiple entries to the same map in multiple sites
        for i, s in enumerate(self.sites):
            for e in entries[i]:
                common.mapAdd(s.cursor, id, e[0], e[1])
        replicateAndMergeAllSites(self.sites)
        allValues = sorted(set([x for l in entries for x in l]))
        compareAllSites(self.sites, common.mapGet, id, common.ReadMode.Lww, allValues, preprocessReads=sorted)

        # test concurrent writes on the same key
        key = secrets.token_urlsafe(10)
        value1 = secrets.token_urlsafe(10)
        value2 = secrets.token_urlsafe(10)
        common.mapAdd(self.sites[0].cursor, id, key, value1)
        sleep(1) # to ensure that value happens after value1 using the physical time
        common.mapAdd(self.sites[1].cursor, id, key, value2)
        sleep(1) # to ensure that the remove happens after the add using the physical time
        common.mapRmv(self.sites[2].cursor, id, key)
        replicateAndMergeAllSites(self.sites)
        allValuesAwMvr = [(x[0], [x[1]]) for x in allValues]
        allValuesAwMvr.append((key, [value1, value2]))
        allValuesAwMvr = sorted(allValuesAwMvr)
        allValuesAwLww = [x for x in allValues]
        allValuesAwLww.append((key, value2))
        allValuesAwLww = sorted(allValuesAwLww)
        allValuesRwMvr = [(x[0], [x[1]]) for x in allValues]
        allValuesRwMvr = sorted(allValuesRwMvr)
        compareAllSites(self.sites, common.mapGet, id, common.ReadMode.AwMvr, allValuesAwMvr, preprocessReads=sorted)
        compareAllSites(self.sites, common.mapGet, id, common.ReadMode.AwLww, allValuesAwLww, preprocessReads=sorted)
        compareAllSites(self.sites, common.mapGet, id, common.ReadMode.RwMvr, allValuesRwMvr, preprocessReads=sorted)
        compareAllSites(self.sites, common.mapGet, id, common.ReadMode.Lww, allValues, preprocessReads=sorted)


    def testCounter(self):
        initSites(self.sites)
        id = '___test_concurrency_counter_' + secrets.token_urlsafe(10)
        counter = 0

        # increment and decrement the same counter concurrently
        for s in self.sites:
            delta = random.randint(1, 10)
            if random.random() < 0.5:
                common.counterInc(s.cursor, id, delta)
                counter += delta
            else:
                common.counterDec(s.cursor, id, delta)
                counter -= delta
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.counterGet, id, None, counter)


    def testList(self):
        initSites(self.sites)
        id = '___test_concurrency_list_' + secrets.token_urlsafe(10)
        list_ = []

        # initialize the list concurrently
        for s in self.sites:
            elem = secrets.token_urlsafe(10)
            common.listAppend(s.cursor, id, elem)
            list_.append(elem)
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.listGet, id, None, list_)

        # insert in the same position
        index = 1
        i = 0
        for s in self.sites:
            elem = secrets.token_urlsafe(10)
            common.listAdd(s.cursor, id, index, elem)
            list_.insert(index + i, elem)
            i += 1
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.listGet, id, None, list_)

        # insert at different positions
        indexes = [random.randint(0, len(list_) - 1) for _ in range(len(list_))]
        indexesList_ = [x for x in indexes]
        for i, s in enumerate(self.sites):
            elem = secrets.token_urlsafe(10)
            common.listAdd(s.cursor, id, indexes[i], elem)
            list_.insert(indexesList_[i], elem)
            # update list_ indexes
            for j in range(i + 1, len(indexesList_)):
                if indexesList_[i] <= indexesList_[j]:
                    indexesList_[j] += 1
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.listGet, id, None, list_)

        # remove the same element
        index = random.randint(0, len(list_) - 1)
        for s in self.sites:
            common.listRmv(s.cursor, id, index)
        del list_[index]
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.listGet, id, None, list_)

        # concurrent insert and remove at the same position
        index = random.randint(0, len(list_) - 1)
        elem = secrets.token_urlsafe(10)
        common.listAdd(self.sites[0].cursor, id, index, elem)
        common.listRmv(self.sites[1].cursor, id, index)
        del list_[index]
        list_.insert(index, elem)
        replicateAndMergeAllSites(self.sites)
        compareAllSites(self.sites, common.listGet, id, None, list_)


    def _testIdempotency(self):
        initSites(self.sites)
        idRegister = '___test_idempotency_register_' + secrets.token_urlsafe(10)
        idSet = '___test_idempotency_set_' + secrets.token_urlsafe(10)
        idMap = '___test_idempotency_map_' + secrets.token_urlsafe(10)
        idCounter = '___test_idempotency_counter_' + secrets.token_urlsafe(10)
        idList = '___test_idempotency_list_' + secrets.token_urlsafe(10)
        register = secrets.token_urlsafe(10)
        set_ = set([secrets.token_urlsafe(10) for _ in range(10)])
        map_ = set([(secrets.token_urlsafe(10), secrets.token_urlsafe(10)) for _ in range(10)])
        counter = 10
        list_ = [secrets.token_urlsafe(10) for _ in range(10)]

        # write data
        common.registerSet(self.sites[0].cursor, idRegister, register)
        for value in set_:
            common.setAdd(self.sites[0].cursor, idSet, value)
        for entry in map_:
            common.mapAdd(self.sites[0].cursor, idMap, entry[0], entry[1])
        common.counterInc(self.sites[0].cursor, idCounter, counter)
        for elem in list_:
            common.listAppend(self.sites[0].cursor, idList, elem)

        # replicate
        replicateAndMergeAllSites(self.sites)

        # duplicate the operations at the remaining sites and execute them
        for s in self.sites:
            s.cursor.execute('INSERT INTO CrdtRemoteOps SELECT * FROM CrdtRemoteOps')
            s.conn.commit()

        replicateAndMergeAllSites(self.sites)

        # check if all sites store the same data
        compareAllSites(self.sites,common.registerGet, idRegister, common.ReadMode.Mvr, [register])
        compareAllSites(self.sites,common.setGet, idSet, common.ReadMode.Lww, sorted(set_), sorted)
        compareAllSites(self.sites,common.mapGet, idMap, common.ReadMode.AwMvr, sorted([(x[0], [x[1]]) for x in map_]), sorted)
        compareAllSites(self.sites,common.counterGet, idCounter, common.ReadMode.Mvr, [counter])
        compareAllSites(self.sites,common.listGet, idList, None, list_)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-H', '--host', type=str, help='Database host', action='store', default='localhost')
    parser.add_argument('-p', '--port', type=str, help='Database port', action='store', default='5432')
    parser.add_argument('-d', '--databases', type=str, help='List of database names (each one will be a different site; minimum of three)', nargs='+', action='store', default=['testdb1', 'testdb2', 'testdb3'])
    parser.add_argument('-u', '--user', type=str, help='Username', action='store', default='postgres')
    parser.add_argument('-P', '--password', type=str, help='Password', action='store', default='postgres')
    parser.add_argument('-m', '--mode', type=str, help='Execution mode: sync vs async writes', action='store', default='sync')
    args, other = parser.parse_known_args()

    databases_set = set()
    databases = [x for x in args.databases if not (x in databases_set or databases_set.add(x))]
    assert len(databases) >= 3, "At least 3 different databases must be provided"

    Test.sites = [Site(args.host, args.port, db, args.user, args.password, args.mode) for db in databases]
    unittest.main(argv=[sys.argv[0]] + other)


if __name__ == '__main__':
    main()
