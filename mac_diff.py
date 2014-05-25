import os
import sqlite3

ROOTDIR = os.path.abspath(os.sep)  # / or drive windows is installed on
DATABASE = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'mac_times.db')


class DBContextManager():
    """Context Manager for sqlite3 database interaction."""
    def __init__(self, db_name):
        self._db_name = db_name
        self._conn = None

    def __enter__(self):
        self._conn = sqlite3.connect(self._db_name)
        self._conn.text_factory = str
        return self._conn

    def __exit__(self, type, value, traceback):
        if type:
            self._conn.rollback()
            print 'Exception: {}'.format(value)
        self._conn.commit()
        self._conn.close()


def main():
    """Creates a database of every file's mac times on system.

    mac_times.db will be dropped in whichever directory the script
    is run from.
    Sample row from db, created on linux 3.13.0-24:
        /sys/devices/virtual/block/ram14/ro|1399820522.72551|1399820522.72551|1399820522.72551

    root@main:~# date --date='@1399820522.72551'
    Sun May 11 10:02:02 CDT 2014
    """
    with DBContextManager(DATABASE) as conn:
        cursor = conn.cursor()
        for root, dirs, files in os.walk(ROOTDIR):
            for file_name in files:
                file_name = os.path.join(root, file_name)
                if os.path.islink(file_name):  # skip symlinks
                    continue
                try:
                    m_time = os.path.getmtime(file_name)
                    a_time = os.path.getatime(file_name)
                    c_time = os.path.getctime(file_name)
                except OSError as ex:
                    return 'Error {}'.format(ex.args[0])
                cursor.execute('INSERT OR IGNORE INTO files VALUES (?,?,?,?)',
                               (file_name, m_time, a_time, c_time))


def create_database():
    """Creates the database, if it's not present."""
    if not os.path.exists(DATABASE):
        with DBContextManager(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE files(filename TEXT unique,'
                           'm_time TEXT, a_time TEXT, c_time TEXT)')

if __name__ == '__main__':
    create_database()
    main()
