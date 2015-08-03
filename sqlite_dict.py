from sqlite3 import connect, Binary
from cPickle import loads, dumps
from os.path import expanduser


class SqliteDict(object):
    def __init__(self, name, deserialize=True):
        if not name.endswith('.db'):
            name = name + '.db'
        self.name = expanduser(name)
        self.conn = connect(self.name)
        cur = self.conn.cursor()
        self.deserialize = deserialize

    def __getitem__(self, key, default=None):
        cur = self.conn.cursor()
        cur.execute("SELECT val FROM kv_store WHERE key=? LIMIT 1", (key,))
        res = cur.fetchone()
        if res is not None:
            if not self.deserialize:
                return str(res[0])
            else:
                return loads(str(res[0]))
        else:
            return default
        
    def get(self, key, default):
        return self.__getitem__(key, default)
    
    def __contains__(self, key):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM kv_store WHERE key=? LIMIT 1", (key,))
        res = cur.fetchone()
        return res[0] != 0

    def values(self):
        cur = self.conn.cursor()
        cur.execute("SELECT val FROM kv_store")
        res = (x[0] for x in cur)
        return res

    def keys(self):
        cur = self.conn.cursor()
        cur.execute("SELECT key FROM kv_store")
        res = (x[0] for x in cur)
        return res

    def items(self):
        cur = self.conn.cursor()
        cur.execute("SELECT key,val FROM kv_store")
        res = (x for x in cur)
        return res
