# Usage: create-schema.py [-h] [-H HOST] [-p PORT] [-d DATABASE] [-u USER] [-P PASSWORD]

import argparse
import psycopg2
import os
import re


def connect(host, port, database, user, password):
    conn = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    conn.autocommit = False
    cursor = conn.cursor()
    return conn, cursor


def dropSchema(host, port, dbname, user, password):
    try:
        conn, cursor = connect(host, port, dbname, user, password)
    except:
        print('Database does not exist.')
        return

    with open('sql/00-drop.sql') as f:
        cursor.execute(f.read())
        conn.commit()


def createSchema(host, port, dbname, user, password, quiet=False):
    conn, cursor = connect(host, port, dbname, user, password)
    folders = ['sql']

    while len(folders) > 0:
        folder = folders.pop(0)
        for name in sorted(os.listdir(folder)):
            fullname = os.path.join(folder, name)
            if os.path.isdir(fullname):
                if re.match(r'^\d', name):
                    folders.append(os.path.join(folder, name))
            else:
                with open(fullname) as f:
                    if not quiet:
                        print(fullname)
                    if '00-drop.sql' in fullname:
                        try:
                            cursor.execute(f.read())
                        except Exception as e:
                            print(e)
                            conn.rollback()
                    else:
                        cursor.execute(f.read())

    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-H', '--host', type=str, help='Database host', action='store', default='localhost')
    parser.add_argument('-p', '--port', type=str, help='Database port', action='store', default='5432')
    parser.add_argument('-d', '--database', type=str, help='Database name', action='store', default='testdb')
    parser.add_argument('-u', '--user', type=str, help='Username', action='store', default='postgres')
    parser.add_argument('-P', '--password', type=str, help='Password', action='store', default='postgres')
    args = parser.parse_args()

    createSchema(args.host, args.port, args.database, args.user, args.password)


if __name__ == '__main__':
    main()
