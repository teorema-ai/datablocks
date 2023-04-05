import base64
import logging
import lzma
import os

import pandas as pd
import sqlalchemy as sa

from . import pals
from .pals import AcquireFailure


logger = logging.getLogger(__name__)


class Locker(pals.Locker):
    def __init__(self, *, user, postgres):
        def create_engine_callable():
            url = f"postgresql://{user}@{postgres['host']}:{postgres['port']}/{postgres['db']}"
            engine = sa.create_engine(url, poolclass=sa.pool.NullPool)
            return engine
        self.__locker = pals.Locker('datablocks', create_engine_callable=create_engine_callable)

    def lock(self, *keys):
       rkeys = [repr(k) for k in keys]
       hash = ",".join(rkeys)
       lock = self.__locker.lock(hash)
       return lock


class Config:
    def __init__(self,
                      *,
                      user,
                      postgres,
                      postgres_schemaname,
                      postgres_tablename,
                      value_column='val',
                      defaults=None,
                      prefix=None):
        self.user = user
        self.postgres = postgres
        self.postgres_schemaname = postgres_schemaname
        self.postgres_tablename = postgres_tablename
        self.value_column = value_column
        self.defaults = defaults
        self.prefix = prefix
        self._db = None
        self._cache = {}
        self._prefix = '' if prefix is None else prefix

    def clone(self,**kwargs):
        _kwargs = dict(user=self.user,
                                postgres=self.postgres,
                                postgres_schemaname=self.postgres_schemaname,
                                postgres_tablename=self.postgres_tablename,
                                value_column=self.value_column,
                                defaults=self.defaults,
                                prefix=self.prefix)
        _kwargs.update(kwargs)
        clone = Config(**_kwargs)
        return clone

    @property
    def db(self):
        if self._db is None:
            postgres = object.__getattribute__(self, 'postgres')
            user = object.__getattribute__(self, 'user')
            pg_host = postgres['host']
            pg_port = postgres['port']
            pg_db = postgres['db']
            self._db = sa.create_engine(f"postgresql+psycopg2://{user}@{pg_host}:{pg_port}/{pg_db}",
                                        isolation_level="SERIALIZABLE")
        return self._db

    def __getattr__(self, attr):
        prefix = object.__getattribute__(self, "prefix")
        prefix_ = f"{prefix}{attr}." if prefix is not None \
                    else f"{attr}."
        ukv = self.clone(prefix=prefix_)
        return ukv

    def __getitem__(self, item):
        _prefix = object.__getattribute__(self, '_prefix')
        _cache = object.__getattribute__(self, '_cache')
        user = object.__getattribute__(self, 'user')

        key = f"{_prefix}{item}"
        if key not in _cache:
            value = self.get(datablocksuser=user, key=key)
            _cache[key] = value
        return _cache[key]

    def __contains__(self, item):
        contains = False
        try:
            self.__getitem__(item)
            contains = True
        except:
            pass
        return contains

    def __call__(self, **keyvals):
        value = self.get(**keyvals)
        return value

    def get(self, **keyvals):
        value = None
        valcol = object.__getattribute__(self, 'value_column')
        db = object.__getattribute__(self, 'db')
        defaults = object.__getattribute__(self, 'defaults')
        postgres_schemaname = object.__getattribute__(self, 'postgres_schemaname')
        postgres_tablename = object.__getattribute__(self, 'postgres_tablename')
        postgres = object.__getattribute__(self, 'postgres')
        with db.connect() as conn:
            clause = ' AND '.join(f"{key} = {repr(val)}" for key, val in keyvals.items())
            default_clause = None
            if defaults is not None:
                keyvals.update(defaults)
                default_clause = ' AND '.join(f"{key} = '{val}'" for key, val in keyvals.items())
            query = f"SELECT {valcol} FROM {postgres_schemaname}.{postgres_tablename} WHERE {clause};"
            r = conn.execute(query)
            rs = r.fetchall()
            if len(rs) == 0 and default_clause is not None:
                default_query = f"SELECT {valcol} FROM {postgres_schemaname}.{postgres_tablename} WHERE {default_clause};"
                r = conn.execute(default_query)
                rs = r.fetchall()
            if len(rs) == 0:
                raise KeyError(f"""No value for keys "{clause}" or "{default_clause}" in {postgres['db']}:{postgres_schemaname}.{postgres_tablename}"""
                                          f"""\n\tquery: {query}\n\tdefault_query: {default_query}""")
            value = rs[0][0]
        return value


class Ids:
    MAX_LEN = 1024

    def __init__(self, dataspace, *, user, postgres, postgres_schemaname, postgres_tablename):
        self.dataspace = dataspace
        self.user = user
        self.postgres = postgres
        self.postgres_schemaname = postgres_schemaname
        self.postgres_tablename = postgres_tablename
        """
        mode: 'file' | 'db'
        """
        self.dataspace.ensure()
        self._init_db_postgres()
        #self.dataspace.acquire()
        #try:
            #self._init_db_postgres()
        #finally:
            #self.dataspace.release()

    def _init_db_postgres(self):
        user = self.user
        postgres = self.postgres
        pg_host = postgres['host']
        pg_port = postgres['port']
        pg_db = postgres['db']
        self._db = sa.create_engine(f"postgresql://{user}@{pg_host}:{pg_port}/{pg_db}",
                                    isolation_level="SERIALIZABLE")
        self._table = f'{self.postgres_schemaname}.{self.postgres_tablename}'
        self._locker = Locker(user=self.user, postgres=self.postgres)

    def get_id(self, key, version, *, unique_hash=True):
        id = self._get_id_db(key, version, unique_hash=unique_hash)
        return id

    def lookup_id(self, id):
        t = self._lookup_id_db(id)
        logger.debug(f"Looked up key id {id}: {t}")
        return t

    def _get_id_db(self, key, version=0, *, unique_hash):
        i = self._dbpg_get_id(self._table, self.dataspace, key, version, unique_hash=unique_hash)
        return i

    def _lookup_id_db(self, id):
        t = self._dbpg_lookup_id(self._table, id)
        return t

    @staticmethod
    def sanitize_value(v, quote=True):
        _v = v.replace("'", '"').replace('%', '<percent>') if isinstance(v, str) else v

        if quote:
            v_ = f"'{_v}'"
        else:
            v_ = _v
        return v_

    def _dbpg_get_id(self, table, dataspace, key, version, *, unique_hash=True):
        _id = None
        # TODO: use the lock context-manager
        # Enforce uniqueness of (dataspace, tag, version) without a b-tree index, which doesn't work for very long tags
        lock = self._locker.lock(dataspace, key, version)
        try:
            with self._db.connect() as conn:
                rs = []
                ds = repr(dataspace)
                _ds = self.sanitize_value(ds)
                _key = self.sanitize_value(key)
                _version = self.sanitize_value(version)
                iquery = f"INSERT INTO {table} (dataspace, key, version, hash) VALUES ({_ds}, {_key}, {_version}, md5({_ds} || {_key} || {_version}))"
                if unique_hash:
                               iquery = iquery + f" ON CONFLICT (hash) DO NOTHING;"
                conn.execute(iquery)
                query = f"SELECT dataspace, key, version, id FROM {table} WHERE dataspace = {_ds} "+\
                                f"AND key = {_key} AND version = {_version}"
                r = conn.execute(query)
                rs = r.fetchall()
                if len(rs) == 0:
                    raise ValueError(f"Failed to insert {version} of key {key} from {dataspace} into table {table}")
                if len(rs) > 1:
                    raise ValueError(f"Multiple ids for version {version} of key {key} from {dataspace} in table {table}:\n"
                                     f"{[r[-1] for r in rs]}")
                if len(rs) == 0:
                    raise ValueError(f"No ids for version {version} of key {key} from {dataspace} in table {table}")
                _id =  rs[0][-1]
                ds_ = self.sanitize_value(ds, quote=False)
                if rs[0][0] != ds_:
                    raise ValueError(f"Database row with retrieved id {_id}: dataspace mismatch: submitted {ds_}, obtained {rs[0][0]}")
                tag_ = self.sanitize_value(key, quote=False)
                if rs[0][1] != tag_:
                    raise ValueError(
                        f"Database row with retrieved id {_id}: key mismatch: submitted {key}, obtained {rs[0][1]}")
                version_ = version
                if rs[0][2] != version_:
                    raise ValueError(
                        f"Database row with retrieved id {_id}: version mismatch: submitted {version_}, obtained {rs[0][2]}")
        except Exception as e:
           raise e
        finally:
            lock.release()
        return _id

    def _dbpg_lookup_id(self, table, _id):
        metadata = None
        try:
            with self._db.connect() as conn:
                rs = []
                query = f"SELECT dataspace, key, version, id FROM {table} WHERE id = {_id}"
                r = conn.execute(query)
                rs = r.fetchall()
                if len(rs) == 0:
                    raise ValueError(f"No id {id}")
                if len(rs) > 1:
                    raise ValueError(f"Multiple records for id {id}")
                metadata = tuple(rs[0][:3])
        except Exception as e:
            raise e
        return metadata

    def frame(self):
        s = self._table.select()
        f = pd.read_sql(s, self._db)
        return f

    def clear(self):
        raise NotImplementedError

    @staticmethod
    def get_hash(key):
        b = key.encode('utf-8')
        c = lzma.compress(b)
        e = base64.b85encode(c)
        s = e.decode('utf-8')
        if len(s) > Ids.MAX_LEN:
            raise ValueError(f"Hash for key {key} has length {len(s)} exceeding MAX_LEN {Ids.MAX_LEN}")
        return s


class Txns:
    def __init__(self, *, user, postgres, postgres_schemaname, postgres_tablename, columns):
        self.user = user
        self.postgres = postgres
        self.postgres_schemaname = postgres_schemaname
        self.postgres_tablename = postgres_tablename
        self.columns = columns
        pg_host = self.postgres['host']
        pg_port = self.postgres['port']
        pg_db = self.postgres['db']
        user = self.user
        self._columns = ", ".join(f'"{c}" VARCHAR' for c in self.columns)
        # TODO: move table setup outside of Python codebase to avoid having to escalate permissions to datablocks?
        _db = sa.create_engine(f"postgresql+psycopg2://datablocks@{pg_host}:{pg_port}/{pg_db}",
                               isolation_level="SERIALIZABLE")
        with _db.connect() as conn:
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {self.postgres_schemaname}.{self.postgres_tablename}
                                            ({self._columns})
                                        ;""")
        # TODO: report table creation only when a table didn't exist before
        logger.debug(f"Ensured db table {self.postgres_schemaname}.{self.postgres_tablename}")
        self._db = sa.create_engine(f"postgresql+psycopg2://{user}@{pg_host}:{pg_port}/{pg_db}",
                                    isolation_level="SERIALIZABLE")

    def add_row(self, **col_values):
        # TODO: avoid building SQL query text, use Sqlalchemy machinery to build the query safely
        def sanitize_tuple(t):
            _t = []
            for v in t:
                v_ = sanitize_value(v)
                _t.append(v_[1:-1])
                t_ = f"""'({", ".join(_t)})'"""
                return t_
        def sanitize_value(v):
            if isinstance(v, tuple):
                return sanitize_tuple(v)
            rv = v if isinstance(v, str) else repr(v)
            _v = rv.replace("'", '"')
            __v = _v.replace("%", "<pct>")
            v_ = f"'{__v}'"
            return v_
        _columns = ", ".join(f'"{k}"' for k in col_values.keys())
        _values = ", ".join(sanitize_value(v) for v in col_values.values())
        with self._db.connect() as conn:
            conn.execute(f"""INSERT INTO {self.postgres_schemaname}.{self.postgres_tablename} ({_columns}) VALUES ({_values})""")

    def frame(self):
        f = pd.read_sql(f'select * from {self.postgres_schemaname}.{self.postgres_tablename}', self._db)
        return f


