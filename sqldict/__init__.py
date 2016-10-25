from sqlite3 import connect
from pickle import loads, dumps
from os.path import expanduser
import sys


def make_sql_table(kv_list, db_name, key_format="String", value_format="BLOB"):
    conn = connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE kv_store (key {} PRIMARY KEY, val  {})'''
              .format(key_format, value_format))
    for k,v in kv_list:
        c.execute('INSERT OR IGNORE INTO kv_store VALUES (?,?)', (k, dumps(v)))

    conn.commit()
    conn.close()
    return conn


class SqlDict(object):
    def __init__(self, name,
                 table_name="kv_store", key_col="key", val_col="val",
                 deserialize=True):
        if not name.endswith('.db'):
            name = name + '.db'
        self.name = expanduser(name)
        self.conn = connect(self.name)
        cur = self.conn.cursor()
        self.deserialize = deserialize
        self.__tablename = table_name
        self.__key_col = key_col
        self.__val_col = val_col

    def __getitem__(self, key, default=None):
        cur = self.conn.cursor()
        cur.execute("SELECT %s FROM %s WHERE %s=? LIMIT 1" %
                    (self.__val_col, self.__tablename, self.__key_col), (key,))
        res = cur.fetchone()
        try:
            if not self.deserialize:
                return res[0]
            else:
                return loads(res[0])
        except:
            raise KeyError

    def get(self, key, default):
        return self.__getitem__(key, default)

    def __contains__(self, key):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM %s WHERE %s=? LIMIT 1" %
                    (self.__tablename, self.__key_col), (key,))
        res = cur.fetchone()
        return res[0] != 0

    def values(self):
        cur = self.conn.cursor()
        cur.execute("SELECT %s FROM %s" % (self.__val_col, self.__tablename))
        res = (x[0] for x in cur)
        return res

    def keys(self):
        cur = self.conn.cursor()
        cur.execute("SELECT %s FROM %s" % (self.__key_col, self.__tablename))
        res = (x[0] for x in cur)
        return res

    def items(self):
        cur = self.conn.cursor()
        cur.execute("SELECT %s,%s FROM %s" %
                    (self.__key_col, self.__val_col, self.__tablename))
        res = (x for x in cur)
        return res
