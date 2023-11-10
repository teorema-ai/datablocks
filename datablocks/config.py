import builtins
import importlib
import io
import json
import logging
import os
import socket
import yaml

import consul

from . import utils


logger = logging.getLogger(__name__)

HOME = os.environ['HOME']

if 'DATABLOCKS_LOG_LEVEL' in os.environ:
    DATABLOCKS_LOG_LEVEL = os.environ['DATABLOCKS_LOG_LEVEL']
else:
    DATABLOCKS_LOG_LEVEL = "INFO"
    
DATABLOCKS_DATALAKE_URL = os.path.join(f'{HOME}', '.cache', 'datalake')
DATABLOCKS_HOMELAKE_URL = os.path.join('{HOME}', '.cache', 'datalake')

DATABLOCKS_CONSUL_HOST = None #'127.0.0.1'
DATABLOCKS_CONSUL_PORT = None #'8500'
if 'DATABLOCKS_CONSUL_HOST' in os.environ:
    DATABLOCKS_CONSUL_HOST = os.environ['DATABLOCKS_CONSUL_HOST']
if 'DATABLOCKS_CONSUL_PORT' in os.environ:
    DATABLOCKS_CONSUL_PORT = os.environ['DATABLOCKS_CONSUL_PORT']


class Consul(consul.Consul):
    def __init__(self, host=DATABLOCKS_CONSUL_HOST, port=DATABLOCKS_CONSUL_PORT, *args):
        super().__init__(host, port, *args)
        self.host = host
        self.port = port
        logging.info(f"CONSUL: DATABLOCKS_CONSUL_HOST=${host}, DATABLOCKS_CONSUL_PORT=${port}")

    # TODO: track path through config tree with recursive 'paths' list parameter
    # TODO: get rid of throw, since 'None' can be a legitimate return value?
    # TODO: Just throw when val at path cannot be obtained.

    def dict_item(self, path):
        colon = path.find(':')
        if colon != -1:
            dictpath = path[:colon]
            item = path[colon+1:]
            dict = CONFIG.dict(dictpath)
            val_ = dict[item]
        else:
            raise ValueError(f"No dict item in path {path}")
        return val_

    def kv_get_val(self, path, *, valtype=None, encoding='yaml', throw=True):
        _val = self.kv.get(path)
        if _val is None or _val[1] is None:
            if throw:
                raise ValueError(f"Failed to get val from path {path}")
            else:
                return None
        val = _val[1]['Value'].decode()
        if encoding is None:
            val_ = val
        elif encoding == 'yaml':
            val_ = yaml.load(io.StringIO(val), Loader=yaml.Loader)
        elif encoding == 'json':
            val_ = json.loads(val)
        else:
            raise ValueError(f"Unknown encoding {encoding}")
        return val_

    def kv_expand_funcarg(self, arg, *, recursive=True, throw=True, context=None):
        if arg[0] in ["'", '"']:
            arg_ = arg[1:-1]
        else:
            arg_ = arg
        _arg = self.kv_expand_val(arg_, recursive=recursive, throw=throw, context=context)
        return _arg

    def kv_expand_funckwarg(self, kwarg, *, recursive=True, throw=True, context=None):
        eq = kwarg.find('=')
        key = kwarg[:eq]
        val = kwarg[eq+1:]
        val_ = self.kv_expand_funcarg(val, recursive=recursive, throw=throw, context=context)
        return key, val_

    def kv_is_funckwarg(self, argstr):
        # no special characters to left of '.'
        eq = argstr.find('=')
        if eq == -1:
            return False
        key = argstr[:eq]
        return not ('.' in key or '(' in key or '$' in key)

    # TODO: disallows combining funcall or listitem with accessors?
    # TODO: allow accessors on local variables only?
    def _kv_expand_funcall(self, val, recursive=True, throw=True, context=None, modloc=0):
        left = val.find('(')
        right = val.rfind(')')
        fqfuncname = val[:left]
        fqfuncparts = fqfuncname.split('.')
        modname = '.'.join(fqfuncparts[:-modloc])
        qfuncparts = fqfuncparts[-modloc:]
        if not (right == len(val)-1 or val[right+1] == '.'):
            raise ValueError(f"Tail of funcall is not an accessor: {val}: {val[right:]}")
        accessors = [_ for _ in val[right+2:].split('.') if len(_) != 0]
        if len(fqfuncparts) > 0 and modname:
            if modname.startswith('$$'):
                mod = context[modname[1:]]
            elif modname.startswith('$'):
                mod = self.kv_expand_val(modname, recursive=recursive, throw=throw, context=context)
            elif len(modname) > 0:
                mod = importlib.import_module(modname)
            else:
                mod = builtins
            parent = mod
            for i in range(len(qfuncparts)):
                childname = qfuncparts[i]
                func = getattr(parent, childname)
                parent = func
        else:
            if fqfuncname.startswith('$$'):
                if fqfuncname not in context:
                    raise ValueError(f"Function {fqfuncname[1:]} not in context {context}")
                func = context[fqfuncname[1:]]
            elif fqfuncname.startswith('$'):
                func = self.kv_expand_val(val, recursive=recursive, throw=throw, context=context)
            else:
                func = globals()[fqfuncname]
        argstring = val[left+1:right]
        argstrs = [_.strip() for _ in argstring.split(',')]
        args = [self.kv_expand_funcarg(argstr, recursive=recursive, throw=throw, context=context) for argstr in argstrs if not self.kv_is_funckwarg(argstr)]
        kwargs_tuples = [self.kv_expand_funckwarg(argstr, recursive=recursive, throw=throw, context=context) for argstr in argstrs if self.kv_is_funckwarg(argstr)]
        kwargs = {k: v for k, v in kwargs_tuples}
        try:
            val_ = func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error evaluating {func} on args={args},  kwargs={kwargs}")
            raise e
        _val = val_
        for accessor in accessors:
            _val = getattr(_val, accessor)
        return _val

    def kv_expand_funcall(self, val, recursive=True, throw=True, context=None):
        left = val.find('(')
        right = val.rfind(')')
        fqfuncname = val[:left]
        fqfuncparts = fqfuncname.split('.')
        fqflen = len(fqfuncparts)
        try:
            for i in range(fqflen - 1):
                try:
                    r = self._kv_expand_funcall(val, recursive=recursive, throw=throw, context=context, modloc=i + 1)
                    return r
                except ModuleNotFoundError:
                    pass
        except Exception as e:
            exc_type, exc_value, exc_traceback = utils.exc_info()
            exc_string = utils.exc_string(exc_type, exc_value, exc_traceback)
            logger.error(f"Failed kv_expand_funcall: {val}")
            print(exc_string)
            raise e

    def kv_expand_item(self, val, recursive=True, throw=True, context=None):
        left = val.find('[')
        right = val.rfind(']')
        lstname = val[:left]
        lst = self.kv_expand_val(lstname, recursive=recursive, throw=throw, context=context)
        if not (right == len(val)-1 or val[right+1] == '.'):
            raise ValueError(f"Tail of listitem is not an accessor: {val}: {val[right:]}")
        accessors = [_ for _ in val[right+2:].split('.') if len(_) != 0]
        argstring = val[left+1:right]
        argstrs = [_.strip() for _  in argstring.split(',')]
        args = [self.kv_expand_funcarg(argstr, recursive=recursive, throw=throw, context=context) for argstr in argstrs if not self.kv_is_funckwarg(argstr)]
        val_ = lst.__getitem__(*args)
        _val = val_
        for accessor in accessors:
            _val = getattr(_val, accessor)
        return _val

    def kv_is_funcall(self, val):
        return '(' in val and ')' in val

    def kv_is_item(self, val):
        return '[' in val and ']' in val

    def kv_is_var(self, val):
        return val.startswith('$') and \
               not self.kv_is_local_var(val) and \
               not self.kv_is_yamlfile(val) and \
               not self.kv_is_object(val)

    def kv_is_local_var(self, val):
        return val.startswith('$$')

    def kv_is_yamlfile(self, val):
        return val.startswith('$yaml:')

    def kv_is_object(self, val):
        return val.startswith('$@')

    def kv_expand_var(self, val, *, valtype=None, recursive=True, throw=True, context=None):
        if self.kv_is_yamlfile(val):
            val_ = self.kv_expand_yamlfile(val)
        elif self.kv_is_funcall(val):
            val_ = self.kv_expand_funcall(val[1:], recursive=recursive, throw=throw, context=context)
        else:
            val_ = self.kv_get_expand_val(val[1:], valtype=valtype, recursive=recursive, context=context)
        return val_

    def kv_expand_yamlfile(self, val):
        prefixlen = len('$yaml:')
        val_ = yaml.load(open(val[prefixlen:], 'r'), Loader=yaml.Loader)
        return val_

    def kv_expand_local_var(self, val, *, valtype=None, recursive=True, throw=True, context=None):
        if '.' in val:
            if '.' in val:
                dot = val.find('.')
                head = val[:dot]
                accessors = [_ for _ in val[dot + 1:].split('.') if len(_) != 0]
                hval = self.kv_expand_val(head, valtype=valtype, recursive=recursive, throw=throw, context=context)
                val_ = hval
                for accessor in accessors:
                    val_ = getattr(val_, accessor)
                return val_
        if context is None or val[1:] not in context:
            raise ValueError(f"No variable {val} in local context {context}")
        else:
            return context[val[1:]]

    def kv_expand_object(self, val):
        path = val[2:]
        obj = self.object(path)
        return obj

    def kv_expand_val(self, val, *, valtype=None, recursive=True, throw=True, context=None):
        throw = False # HACK
        if val is None:
            if throw:
                raise ValueError(f"Cannot expand a None val")
            else:
                return None
        if isinstance(val, str):
            if self.kv_is_item(val):
                val_ = self.kv_expand_item(val, recursive=recursive, throw=throw, context=context)
            elif self.kv_is_funcall(val):
                val_ = self.kv_expand_funcall(val, recursive=recursive, throw=throw, context=context)
            elif self.kv_is_local_var(val):
                val_ = self.kv_expand_local_var(val, valtype=valtype, recursive=recursive, throw=throw, context=context)
            elif self.kv_is_var(val):
                val_ = self.kv_expand_var(val, valtype=valtype, recursive=recursive, context={})
            elif self.kv_is_object(val):
                val_ = self.kv_expand_object(val)
            elif self.kv_is_yamlfile(val):
                val_ = self.kv_expand_yamlfile(val)
            elif valtype is not None:
                val_ = valtype(val)
            else:
                val_ = val
            return val_
        elif isinstance(val, dict):
            val_ = {}
            for k, v in val.items():
                vtype = valtype[k] if valtype is not None else None
                v_ = self.kv_expand_val(v, valtype=vtype, recursive=recursive, context=context)
                if self.kv_is_var(k):
                    context[k] = v_
                else:
                    val_[k] = v_
        elif isinstance(val, list):
            val_ = []
            for i, v in enumerate(val):
                vtype = valtype[i] if valtype is not None else None
                v_ = self.kv_expand_val(v, valtype=vtype, recursive=recursive, context=context)
                val_.append(v_)
        else:
            val_ = val
        return val_

    def kv_get_expand_dict(self, path, *, format='yaml', throw=True, with_context=False):
        config_kwargs = self.kv_get_dict(path, format=format)
        kwargs = {}
        context = {}
        for key, val in config_kwargs.items():
            val_ = self.kv_expand_val(val,  recursive=True, throw=throw, context=context)
            if key.startswith('$'):
                context[key] = val_
            else:
                kwargs[key] = val_
        if with_context:
            kwargs.update(context)
        return kwargs

    def kv_get_keys(self, path):
        if path[-1] != '/':
            path = path + "/"
        index, keys = self.kv.get(path, keys=True)
        lenkey = len(path)
        _keys = [k[lenkey:] for k in keys]
        return _keys

    def kv_get_kwargs(self, path):
        keys = self.kv_get_keys(path)
        kwargs = {key: self.kv_get_val(key) for key in keys}
        return kwargs

    def kv_has_key(self, path, key):
        if path[-1] != '/':
            path = path + "/"
        keys = self.kv_get_keys(path)
        for k in keys:
            if k.startswith(key):
                return True
        return False

    @staticmethod
    def kv_class_path(obj):
        cls = obj.__class__
        modname = cls.__module__
        clsname = cls.__qualname__
        fqclsname = f"{modname}.{clsname}"
        fqclsparts = fqclsname.split('.')
        class_config_key = '/'.join(fqclsparts)
        return class_config_key

    @staticmethod
    def kv_obj_path(obj, objkey):
        if objkey is None:
            return None
        obj_config_key = f"{Config.kv_class_path(obj)}/{objkey}"
        return obj_config_key

    # TODO: remove recursive, throw and context (set to (True, True, {})
    def kv_get_expand_val(self, path, *, valtype=None, recursive=True, throw=True, context=None):
        if path is None:
            if throw:
                raise ValueError(f"Cannot get and expand value from None path")
            else:
                return None
        _val = self.kv_get_val(path, throw=throw)
        val = self.kv_expand_val(_val, valtype=valtype, recursive=recursive, throw=throw, context=context)
        return val

    def kv_get_dict(self, path, format='yaml'):
        _format = format.lower()
        configstr = self.kv_get_val(path, encoding=None)
        if _format == 'yaml':
            configio = io.StringIO(configstr)
            config = yaml.load(configio, Loader=yaml.Loader)
        elif _format == 'json':
            config = json.loads(configstr)
        else:
            raise ValueError(f"Unsupported format {format}")
        return config

    def val(self, key):
        val = self.kv_get_expand_val(key)
        return val

    def dict(self, key, *, with_context=True):
        dict = self.kv_get_expand_dict(key, with_context=with_context)
        return dict

    def object(self, url):
        dict = self.kv_get_expand_dict(url)
        if len(dict) != 1:
            raise ValueError(f"Too many keys in object config {url}")
        url = list(dict.keys())[0]
        object = dict[url]
        return object

    def item(self, url, *, with_context=True):
        sep = url.find(':')
        if sep != -1:
            path = url[:sep]
            key = url[sep+1:]
            dct = self.dict(path, with_context=with_context)
            item = dct[key]
        else:
            item = self.dict(url)
        return item



def DICT(**kwargs):
    d = dict(**kwargs)
    return d


def CONCAT(*ss):
    s = ''.join(ss)
    return s



# TODO: Use DATABLOCKS_POSTGRES_HOST and DATABLOCKS_POSTGRES_PORT ENV to set up CONFIG_CLASS.POSTGRES dict
# FIX: clear out most of CONFIG_CLASS, leaving only Consul and db params (extractable from Consul).
class Config(Consul):

    # REMOVE: used in pool to validate tasks, so remove from there as well
    SIDE = None

    def __init__(self, *args):
        super().__init__(*args)
        self._user = None

        self.DATABLOCKSSVR = self.host
        # TODO: obtain the full POSTGRES config (host, port, db) from POSTGRES['Q']
        self.POSTGRES = self.kv_get_dict('datablocks/config/POSTGRES')

        if self._user is None:
            if 'USER' in os.environ:
                self.USER = os.environ['USER']
            else:
                try:
                    self.USER = os.getlogin()
                except:
                    self.USER = self._user
        else:
            self.USER = self._user

        if self.USER is None:
            self.USER = 'root'
        self.DEV = \
            ('DATABLOCKS_DEV' in os.environ and \
             os.environ['DATABLOCKS_DEV'].lower() in ['true', 'yes', '1']) or \
            ('DATABLOCKSDEV' in os.environ and \
             os.environ['DATABLOCKSDEV'].lower() in ['true', 'yes', '1']) or \
            ('DATABLOCKS_DEV' in os.environ and \
             os.environ['DATABLOCKS_DEV'].lower() in ['true', 'yes', '1'])

        if 'HOSTNAME' in os.environ:
            self.HOSTNAME = os.environ['HOSTNAME']
        else:
            self.HOSTNAME = socket.getfqdn()
        self.UHOSTNAME = self.HOSTNAME
        if 'UHOSTNAME' in os.environ:
            self.UHOSTNAME = os.environ['UHOSTNAME']
        self.HOME = os.path.expanduser('~')


if DATABLOCKS_CONSUL_HOST is not None:
    CONSUL = Consul()
    CONFIG = Config()
    CONSUL = CONFIG


def check_config(*ignored, **kwignored):
    config = {}
    import sys
    import datablocks
    import os
    config['sys.path'] = sys.path
    config['datablocks.__path__'] = datablocks.__path__
    umask = os.umask(0o002)
    umask_ = os.umask(umask)
    config['os_mask_consistence']  = umask == umask_
    config['os.umask'] = umask
    config['CONFIG.DEV'] = CONFIG.DEV
    config['DATABLOCKS_CONSUL_HOST'] = DATABLOCKS_CONSUL_HOST
    config['DATABLOCKS_CONSUL_PORT'] = DATABLOCKS_CONSUL_PORT
    config['HOSTNAME'] = CONFIG.HOSTNAME
    config['UHOSTNAME'] = CONFIG.UHOSTNAME
    config['POSTGRES'] = CONFIG.POSTGRES
    return config


