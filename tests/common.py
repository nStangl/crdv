from enum import Enum
import time
import psycopg2
from psycopg2.extras import register_composite

class ReadMode(Enum):
    Mvr = 1
    Lww = 2
    Aw = 3
    Rw = 4
    RwMvr = 5
    AwMvr = 6
    AwLww = 7


def connect(host, port, database, user, password):
    conn = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    conn.autocommit = False
    return conn


# if args is not None, we setup the static variables,
# otherwise we call the connect method
def connection(args=None):
    if args is not None:
        connection.host = args.host
        connection.port = args.port
        connection.database = args.database
        connection.user = args.user
        connection.password = args.password
    else:
        assert connection.host is not None, "Connection variables not set up."
    
    return connect(connection.host, connection.port, connection.database,
        connection.user, connection.password)


def initSite(conn):
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT initSite(1)')
        conn.commit()
    except:
        conn.rollback()
    cursor.close()


def prepareTypes(conn):
    register_composite('mEntry', conn, globally=True)
    register_composite('mEntryMvr', conn, globally=True)


def prepareMode(conn, mode):
    assert mode in ('sync', 'async')
    cursor = conn.cursor()

    if mode == 'sync':
        cursor.execute("select switch_read_mode('local')")
        cursor.execute("select switch_write_mode('sync')")
    else:
        cursor.execute("select switch_read_mode('all')")
        cursor.execute("select switch_write_mode('async')")

    cursor.close()
    conn.commit()


########## Register ##########


def registerGet(cursor, id, mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.Mvr, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select register{mode.name}Get(%s)', (id,))
    return cursor.fetchone()[0]


def registerSet(cursor, id, value):
    cursor.execute('select registerSet(%s, %s)', (id, value))


def registerGetAll(cursor, idLike='', mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.Mvr, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select id, data from register{mode.name} where id like %s', (idLike,))
    return cursor.fetchall()


########## Set ##########


def setGet(cursor, id, mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.Aw, ReadMode.Rw, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select set{mode.name}Get(%s)', (id,))
    return cursor.fetchone()[0]


def setContains(cursor, id, value, mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.Aw, ReadMode.Rw, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select set{mode.name}Contains(%s, %s)', (id, value))
    return cursor.fetchone()[0]


def setAdd(cursor, id, value):
    cursor.execute('select setAdd(%s, %s)', (id, value))


def setRmv(cursor, id, value):
    cursor.execute('select setRmv(%s, %s)', (id, value))


def setGetAll(cursor, idLike='', mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.Aw, ReadMode.Rw, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select id, data from set{mode.name}Tuple where id like %s', (idLike,))
    return cursor.fetchall()


def setClear(cursor, id):
    cursor.execute('select setClear(%s)', (id,))


########## Map ##########


def mapGet(cursor, id, mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.AwMvr, ReadMode.AwLww, ReadMode.RwMvr, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select map{mode.name}Get(%s)', (id,))
    return cursor.fetchone()[0]


def mapValue(cursor, id, key, mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.AwMvr, ReadMode.AwLww, ReadMode.RwMvr, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select map{mode.name}Value(%s, %s)', (id, key))
    return cursor.fetchone()[0]


def mapContains(cursor, id, key, mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.AwMvr, ReadMode.AwLww, ReadMode.RwMvr, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select map{mode.name}Contains(%s, %s)', (id, key))
    return cursor.fetchone()[0]


def mapAdd(cursor, id, key, value):
    cursor.execute('select mapAdd(%s, %s, %s)', (id, key, value))


def mapRmv(cursor, id, key):
    cursor.execute('select mapRmv(%s, %s)', (id, key))


def mapGetAll(cursor, idLike='', mode: ReadMode = ReadMode.Lww):
    assert mode in (ReadMode.AwMvr, ReadMode.AwLww, ReadMode.RwMvr, ReadMode.Lww), "Mode not supported"
    cursor.execute(f'select id, data from map{mode.name}Tuple where id like %s', (idLike,))
    return cursor.fetchall()


def mapClear(cursor, id):
    cursor.execute('select mapClear(%s)', (id,))


########## Counter ##########


def counterGet(cursor, id):
    cursor.execute(f'select counterGet(%s)', (id,))
    return cursor.fetchone()[0]


def counterInc(cursor, id, delta):
    cursor.execute('select counterInc(%s, %s)', (id, delta))


def counterDec(cursor, id, delta):
    cursor.execute('select counterDec(%s, %s)', (id, delta))


def counterGetAll(cursor, idLike):
    cursor.execute(f'select id, data from Counter where id like %s', (idLike,))
    return cursor.fetchall()


########## List ##########


def listGet(cursor, id):
    cursor.execute('select listGet(%s)', (id,))
    return cursor.fetchone()[0]


def listGetAt(cursor, id, index):
    cursor.execute('select listGetAt(%s, %s)', (id, index))
    return cursor.fetchone()[0]


def listAdd(cursor, id, index, value):
    cursor.execute('select listAdd(%s, %s, %s)', (id, index, value))


def listAppend(cursor, id, value):
    cursor.execute('select listAppend(%s, %s)', (id, value))


def listRmv(cursor, id, index):
    cursor.execute('select listRmv(%s, %s)', (id, index))


def listGetAll(cursor, idLike):
    cursor.execute(f'select id, data from listTuple where id like %s', (idLike,))
    return cursor.fetchall()


def listClear(cursor, id):
    cursor.execute('select listClear(%s)', (id,))


########## Replication ##########


def _getReplicationStatus(cursor):
    cursor.execute('select current_database()')
    db = cursor.fetchone()[0]
    cursor.execute(f'''
        select slot_name, confirmed_flush_lsn
        from pg_replication_slots
        where database = '{db}'
    ''')

    return {x[0]: x[1] for x in cursor.fetchall()}


def replicate(cursor):
    cursor.execute('SELECT replicate()')
    
    # wait for all remote data to be received first
    prev = {}
    curr = _getReplicationStatus(cursor)
    while prev != curr:
        time.sleep(0.2)
        prev = curr
        curr = _getReplicationStatus(cursor)


def merge(cursor):
    cursor.execute('SELECT merge()')
