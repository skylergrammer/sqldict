from sqlite3 import connect
import pickle
from os.path import expanduser
import sys


def make_sql_table(kv_list, db_name, key_format="String", value_format="BLOB", serializer=pickle):
    with connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE kv_store (key {} PRIMARY KEY, val  {})'''
                    .format(key_format, value_format))
        for k,v in kv_list:
            if serializer is None:
                cur.execute('INSERT OR IGNORE INTO kv_store VALUES (?,?)', (k, v))
            else:
                cur.execute('INSERT OR IGNORE INTO kv_store VALUES (?,?)', (k, serializer.dumps(v)))


class SqlDict(object):
    def __init__(self, name,
                 table_name="kv_store", key_col="key", val_col="val",
                 serializer=pickle):
        if not name.endswith('.db'):
            name = name + '.db'
        self.name = expanduser(name)
        self.serializer = serializer
        assert hasattr(serializer, 'loads')
        assert hasattr(serializer, 'dumps')
        self.__tablename = table_name
        self.__key_col = key_col
        self.__val_col = val_col

    def __getitem__(self, key):
        with connect(self.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT {} FROM {} WHERE {}=? LIMIT 1"
                        .format(self.__val_col, self.__tablename, self.__key_col), (key,))
            res = cur.fetchone()
        try:
            if self.serializer is None:
                return res[0]
            else:
                return self.serializer.loads(res[0])
        except:
            raise KeyError

    def __setitem__(self, key, value):
        with connect(self.name) as conn:
            cur = conn.cursor()
            if self.serializer is not None:
                value = self.serializer.dumps(value)
            else:
                value = value
            cur.execute('INSERT OR REPLACE INTO {} VALUES (?,?)'.format(self.__tablename),
                        (key, value))

    def get(self, key, default):
        return self.__getitem__(key, default)

    def __contains__(self, key):
        with connect(self.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM {} WHERE {}=? LIMIT 1"
                        .format(self.__tablename, self.__key_col), (key,))
            res = cur.fetchone()
        return res[0] != 0

    def values(self):
        with connect(self.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT {} FROM {}"
                        .format(self.__val_col, self.__tablename))
            if self.serializer is None:
                res = (x[0] for x in cur)
            else:
                res = (self.serializer.loads(x[0]) for x in cur)
        return res

    def keys(self):
        with connect(self.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT {} FROM {}".format(self.__key_col, self.__tablename))
            res = (x[0] for x in cur)
        return res

    def items(self):
        with connect(self.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT {},{} FROM {}"
                        .format(self.__key_col, self.__val_col, self.__tablename))
            if self.serializer is None:
                res = ((k, v) for k,v in cur)
            else:
                res = ((k, self.serializer.loads(v)) for k,v in cur)
        return res

    def __len__(self):
        with connect(self.name) as conn:
            cur = conn.cursor()
            cur.execute("SELECT Count(*) FROM {}".format(self.__tablename))
            res = cur.fetchone()[0]
        return res
