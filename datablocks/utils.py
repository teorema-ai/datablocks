import builtins
from contextlib import contextmanager
import datetime
import hashlib
import io
import logging
import os
from time import perf_counter

import subprocess
import sys
import time
import traceback

import git 

import numpy as np
import pandas as pd

import pyarrow as pa
import pyarrow.parquet


logger = logging.getLogger(__name__)


def serializable(_):
    return _


def DEPRECATED(_):
    return _


def OVERRIDE(_):
    return _


def OVERRIDEN(_):
    return _


def REPLACE(_):
    return _

def RENAME(_):
    return _

def REMOVE(_):
    return _


def ALIAS(_):
    return _


def EXTRA(_):
    return _


def BROKEN(_):
    logging.warning(f"BROKEN: {_}")
    return _


def get_logger(logger, _globals):
    if logger is None:
        logger = _globals['logger']
    return logger


def log(file_or_paths, *, position=0, size=None):
    close_on_exit = False
    if hasattr(file_or_paths, 'read'):
        file = file_or_paths
        path = None
        close_on_exit = False
    else:
        path = file_or_paths
        if isinstance(path, pd.DataFrame):
            logs = {row[0]: log(row[1]) for row in path.iterrows()}
            return logs
        if isinstance(path, pd.Series):
            path = path.log
        file = open(path, 'r')
        close_on_exit = True
    s = None
    try:
        if position is not None:
            if path is not None:
                statinfo = os.stat(path)
                fsize = statinfo.st_size
                if position < -1:
                    file.seek(max(-1, fsize + position), os.SEEK_SET)
                else:
                    file.seek(min(position, fsize), os.SEEK_SET)
            else:
                file.seek(position, os.SEEK_SET)
        if size is not None:
            s = file.read(size)
        else:
            s = file.read()
    finally:
        if close_on_exit and file is not None:
            file.close()
    return s


def Dictionary(dict):
    def set(self, key, val):
        self[key] = val
        return self


def date_range(start, end=None):
    if start is None:
        raise ValueError(f"Empty date range")
    start = pd.to_datetime(to_datetime64(start)).date()
    if end is None:
        end = start
    end = pd.to_datetime(to_datetime64(end)).date()
    return [np.datetime64(start+pd.Timedelta('1 days')*n) for n in range((end-start).days+1)]


def datestr_range(start, end=None):
    dates = date_range(start, end)
    datestrs = [pd.to_datetime(date).strftime('%Y-%m-%d') \
                 for date in dates]
    return datestrs


def datestr_kvchain_range(start, end=None):
    dates = date_range(start, end)
    datestrs = [(('date', pd.to_datetime(date).strftime('%Y-%m-%d')),)\
                                                            for date in dates]
    return datestrs


class date_range_generator:
    def __init__(self, *, start, end=None):
        self.start = start
        self.end = end

    def __iter__(self):
        _date_range = date_range(self.start, self.end)
        _iterator = iter(_date_range)
        return _iterator

    def __repr__(self):
        _repr = f"date_range_generator(start={repr(self.start)}, end={repr(self.end)})"
        return _repr

    def __tag__(self):
        _tag = f"date_range_generator[start={repr(self.start)},end={repr(self.end)}]"
        return _tag


def date_instruments_dict(datestrs, instruments):
    date_instruments_dict = {date: instruments for date in datestrs}
    return date_instruments_dict


def date_time64(date, time, *, shift='0 days'):
    date_ = pd.to_datetime(date)+pd.to_timedelta(shift)
    datestr = pd.to_datetime(date_).strftime('%Y-%m-%d')
    timestr = pd.to_datetime(time).strftime('%H:%M:%S')
    dt = np.datetime64(f"{datestr}T{timestr}")
    return dt


def to_datetime64(s):
    if s == 'today':
        dt64 = np.datetime64(s)
    elif s == 'yesterday':
        dt64_ = np.datetime64('today')
        dt_ = pd.to_datetime(dt64_)
        td = pd.Timedelta('1 days')
        dt = dt_ - td
        dt64 = np.datetime64(dt)
    elif s == 'a_week_ago':
        dt64_ = np.datetime64('today')
        dt_ = pd.to_datetime(dt64_)
        td = pd.Timedelta('7 days')
        dt = dt_ - td
        dt64 = np.datetime64(dt)
    else:
        ss = str(s)
        dt = pd.to_datetime(ss)
        dt64 = np.datetime64(dt)
    return dt64


def to_datestr(s):
    return pd.to_datetime(to_datetime64(s)).strftime('%Y-%m-%d')


def tintervals(start, end, step='1 hours', overlap='0 minutes'):
    end = pd.to_datetime(end)
    dt = pd.to_timedelta(step)
    ov = pd.to_timedelta(overlap)
    t0 = pd.to_datetime(start)
    t1 = t0+dt
    while t1 <= end:
        yield (np.datetime64(t0), np.datetime64(t1+ov))
        t0 = t1
        t1 += dt


def day_snaps(date, freq):
    start = pd.to_datetime(date)
    end = start + pd.Timedelta('1 days')
    day = end - start
    delta = pd.Timedelta(freq)
    n = int(day/delta)+1
    snaps = [np.datetime64(start+delta*i) for i in range(n)]
    return snaps


EPOCH = np.datetime64('1970-01-01 00:00:00.000000000')
SEC = np.timedelta64(1, 's')
NSEC = np.timedelta64(1, 'ns')
DAY = np.timedelta64(24, 'h')
HOUR = np.timedelta64(1, 'h')


def microseconds_since_epoch():
    epoch = datetime.datetime.utcfromtimestamp(0)
    now = datetime.datetime.now()
    dt = now - epoch
    _ = dt.total_seconds()*1e+6 + dt.microseconds
    return _


def datetime_to_microsecond_str():
    _ =  datetime.datetime.now().strftime('%Y-%m-%d.%H.%M.%S.%s')
    return _


def pair2dt(total_seconds, nanoseconds, *, nat_when_invalid=True):
    try:
        dt = pd.to_datetime(total_seconds, unit='s')
        td = pd.to_timedelta(nanoseconds, unit='ns')
        t = dt+td
    except:
        t = pd.NaT
    return t


def dt2pair(dt):
    td = dt-EPOCH
    secs, nsecs = td//SEC, td%NSEC
    return secs, nsecs.astype('int64')


def identity(_):
    return _


def NoneFunc(*args, **kwargs):
    return None


def TrueFunc(*args, **kwargs):
    return True


def TrueFuncFactory(*args, **kwargs):
    return TrueFunc


def dict_key_underscore_reducer(a,b):
    return b if a is None else a if b is None else f'{a}_{b}'


def dict_key_join_reducer(a,b):
    return b if a is None else a if b is None else f'{a}{b}'


def dict_key_tuple_reducer(a,b):
    return (b,) if a is None else (a,) if b is None else a+(b,)


def flatten_dict(d, reducer=dict_key_underscore_reducer):
    import flatten_dict as flattendict
    return flattendict.flatten(d, reducer=reducer)


def flatten_dict_tuple_keys(d):
    return flatten_dict(d, dict_key_tuple_reducer)


def flatten_dicts(dicts, reducer=dict_key_underscore_reducer):
    import flatten_dict as flattendict
    return (flattendict.flatten(d, reducer=reducer) for d in dicts)


def df2str(df):
    buf = io.StringIO()
    df.to_csv(buf)
    s = buf.getvalue()
    return s


def str2df(s, *, dtypes, date_parser=pd.to_datetime, **kwargs):
    tlist = ['datetime64[ns]', 'datetime64[ms]']
    otypes_ = {k: v for k, v in dtypes.items() if v not in tlist}
    ttypes_ = {k: 'str' for k, v in dtypes.items() if v in tlist}
    dtypes_ = {**otypes_, **ttypes_}
    buf = io.StringIO(s)
    df = pd.read_csv(buf, dtype=dtypes_, parse_dates=list(ttypes_.keys()), date_parser=date_parser, **kwargs)
    return df


def dfdiff(df1, df2, float_cols=[], equal_cols=None, tol=1e-8):
    if equal_cols is None:
        equal_cols = [c for c in df1.columns if c not in float_cols]
    r1 = df1[equal_cols].equals(df2[equal_cols])

    r2 = ((df1[float_cols]-df2[float_cols]).fillna(0.0).abs() < tol).all().all()
    return r1 and r2


def normalize_string(product, *, replacements={':': '.', ' ': '__'}):
    for k, v in replacements.items():
        product = product.replace(k, v)
    return product


def path_split(path, keep_leading=False):
    parts_ = path.split(os.sep)
    if not keep_leading and parts_[0] == '':
        parts = parts_[1:]
    else:
        parts = parts_
    return parts


def runcmd(args,
                   *,
                   cwd=None,
                   out=None,
                   err=None,
                   timeout_secs=-1,
                   shell=True,
                   split_args=False,
                   wait_on_exit_secs=2):
    if isinstance(args, str):
        if split_args:
            args = args.split()
    if isinstance(args, list):
        if not split_args:
            args = ' '.join(args)
    timeout = timeout_secs
    if timeout < 0:
        timeout = None
    p = subprocess.Popen(args, cwd=cwd, shell=shell, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, env=os.environ)
    if err is None:
        err = out
    if out is not None:
        s = None
        while s is None:
            out_, err_ = p.communicate(timeout=timeout)
            if out_ is not None:
                outlines = out_.decode().split('\n')
                for outline in outlines:
                    out(outline)
            if err_ is not None:
                errlines = err_.decode().split('\n')
                for errline in errlines:
                    err(errline)
            s = p.poll()
    r = p.wait(timeout=timeout)
    time.sleep(wait_on_exit_secs)
    return r


def renumerate(lst):
    dct = {p: i for i, p in enumerate(lst)}
    return dct


def set_attrs(o, *, include=None, exclude=[], **kwargs):
    if include is not None:
        keys = [k for k in kwargs.keys() if k in include and not k in exclude]
    else:
        keys = [k for k in kwargs.keys() if k not in exclude]
    for k in keys:
        o.setattr(k, kwargs[k])


def exc_info():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return exc_type, exc_value, exc_traceback


def exc_string(exc_type, exc_value, exc_traceback):
    tb_lines = traceback.format_tb(exc_traceback)
    exc_traceback = '\n'.join(tb_lines)
    es = f"{exc_traceback}{exc_type}: {exc_value}"
    return es


def exc_traceback_string(exc_traceback):
    tb_lines = traceback.format_tb(exc_traceback)
    exc_traceback = '\n'.join(tb_lines)
    return exc_traceback


def len(f):
    if f is None:
        return None
    return builtins.len(f)


def test_assert(expected, result):
    assert (expected == result), f"Expected {expected}, got {result}"


def DATE(*, delay_days):
    start0 = datetime.datetime.today().date()
    if delay_days > 0:
        start_ = start0 - datetime.timedelta(days=delay_days)
    else:
        start_ = start0
    start = np.datetime64(pd.to_datetime(start_).date())
    return start


def BOOL(val):
    if isinstance(val, str):
        return val.lower() in ['true', 'yes', '1']
    return bool(val)


def NONE(val):
    if isinstance(val, str):
        if val.lower() in ['none', '0', 'no']:
            return None
    return val


@contextmanager
def timer_secs() -> float:
    '''
        Example:
            with timer_secs() as my_timer:
                time.sleep(10)
                print(f"Sleeping took {my_timer} secs")
    '''
    start = perf_counter()
    yield lambda: perf_counter() - start


def func_type(func):
    ftype = None
    if '.' not in func.__qualname__:
        ftype = 'function'
    else:
        # __qualname__: 'className.functioNname'
        cls_name = func.__qualname__.rsplit('.', 1)[0]
        # Get the class by name
        cls = getattr(sys.modules[func.__module__], cls_name)
        # cls.__dict__[func.__name__] should return like <class 'staticmethod'>
        ftype = cls.__dict__[func.__name__].__class__.__name__
        if not ftype in ['staticmethod', 'classmethod', 'instance-method']:
            raise TypeError('Unknown Type %s, Please check input is method or function' % func)
    return ftype


def truncate_str(s, max_len=None, *, use_ellipsis=False):
    _s = s[:max_len] if max_len else s
    if len(_s) < len(s) and use_ellipsis:
        _s = _s + "..."
    return _s


def datetime_now_key():
    now = datetime.datetime.now()
    key = now.strftime('%Y-%m-%d.%H.%M.%S')
    return key


def key_to_id(key):
    namespace_bytes = key.encode()
    """
    #namespace_uuid = uuid.UUID(bytes=namespace_bytes[:16]).int
    namespace_uuid = uuid.UUID(bytes=namespace_bytes)
    _uuid = uuid.uuid5(namespace_uuid, key).int
    id = _uuid + version
    """
    hashstr = hashlib.sha1(namespace_bytes).hexdigest()
    maxint64 = int(2**63)
    id = int(hashstr, 16)%maxint64
    return id


def docstr(docstr):
    def decorator(f):
        f.__doc__ = docstr
        return f
    return decorator


def setup_repo(repo, revision, *, verbose=False):
        if repo is not None:
            repo = git.Repo(repo)
            if verbose:
                print(f"Using git repo {repo}")
            if repo.is_dirty():
                raise ValueError(f"Dirty git repo: {repo}: commit your changes")
            if revision is not None:
                #TODO: lock repo and unlock in __delete__
                #TODO: if locked, print warning
                #TODO: locking DB should identify the lock owner and start time, 
                #TODO: so print that warning
                repo.checkout(revision)
                if verbose:
                    print(f"Using git revision {revision}")


class RepoMixin:
    def setup_repo(self):
        verbose = False if not getattr(self, 'revision') else self.revision
        setup_repo(self.repo, self.revision)
    


