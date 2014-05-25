import os
import sqlite3

#TODO: if a filename is already there, update it to the new mac times, but keep a dated version?

ROOTDIR = os.path.abspath(os.sep)  # / or drive windows is installed on
DATABASE = os.path.join(os.path.expanduser('~'), 'mac_times.db')

class DBContextManager():
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
            self._conn.close()

def main():
    with DBContextManager(DATABASE) as conn:
        cursor = conn.cursor()
        for root, dirs, files in os.walk(ROOTDIR):
            for file_name in files:
                file_name = os.path.join(root, file_name)
                if os.path.islink(file_name):
                    continue
                try:
                    m_time = os.path.getmtime(file_name)
                    a_time = os.path.getatime(file_name)
                    c_time = os.path.getctime(file_name)
                except OSError as ex:
                    return 'Error {}'.format(ex.args[0])
                cursor.execute('INSERT into files values (?,?,?,?)',
                               (file_name, m_time, a_time, c_time))
        conn.commit()

def create_database():
    if not os.path.exists(DATABASE):
        with DBContextManager(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE files(filename TEXT unique,'
                           'm_time TEXT, a_time TEXT, c_time TEXT)')
            conn.commit()

create_database()
main()
