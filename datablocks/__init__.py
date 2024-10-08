from importlib import import_module
import logging
import os
import re
import sys

import pdb

import regex


from datablocks.config import DATABLOCKS_LOG_LEVEL
logging.basicConfig(level=DATABLOCKS_LOG_LEVEL)

from .dataspace import Dataspace, DATABLOCKS_DATALAKE, DATABLOCKS_HOMELAKE
from .dbx import *


def sprint(x, indent=0):
    prefix = "".join(['\t']*indent)
    if isinstance(x, dict):
        s = ""
        for k, v in x.items():
            s += f"{prefix}{k}: "
            if isinstance(v, dict):
                s += "\n" + sprint(v, indent=indent+1)
            else:
                s += f"{v}"
            s += "\n"
    else:
        s = f"{prefix}{x}"
    return s


def pprint(x, indent=0):
    print(sprint(x, indent=indent))


def print_usage(*, console=True):
    if console:
        print(f"""Usage:\nDATALAKE_URL={{DATALAKE_URL}}\\\n"""
                      f"""{sys.argv[0]} --help | \n[package.module, package.module, ...] {{package.module.Class}}({{key}}={{val}},...).{{method}}({{key}}={{val}},...)""")
    else:
        print(f"""Usage:\n{__name__}.exed("--help")  |\n{__name__}.exec("{{package.module.Class}}({{key}}={{val}},...).{{method}}({{key}}={{val}},...)""")


def debug(argstr=None):
    return exec(argstr, debug=True)


def exec_print(argstr=None):
    pprint(exec(argstr))


def debug_print(argstr=None):
    pprint(debug(argstr))


def exec(argstr=None, *, debug=False):
    import datablocks.dbx
    def import_mod(path):
        path_parts = path.split('.')
        for i in range(1, len(path_parts)+1):
            _path = '.'.join(path_parts[:i])
            module = __import__(_path, globals(), locals())
            globals()[_path] = module
    if argstr is None:
        console = True
        if len(sys.argv) > 2:
            print_usage()
            raise ValueError(f"Too many args: {sys.argv}")
        elif len(sys.argv) == 1:
            print_usage()
            raise ValueError(f"Too few args: {sys.argv}")
        argstr = sys.argv[1]
    else:
        console = False

    if argstr.strip() == '--help':
        print_usage(console=console)
        if console:
            sys.exit(0)
        else:
            return

    _eval = __builtins__['eval']
    istr = argstr.strip()
    if istr.startswith('['):
        imports_start = 1
        imports_end = istr.find(']')
        if imports_end == -1:
            raise ValueError(f"Malformed imports specification: {istr}")
        imports = istr[imports_start:imports_end]
        module_names = [i.strip() for i in imports.split(',')]
        for module_name in module_names:
            import_mod(module_name)
        s = istr[imports_end+1:].strip()
    else:
        s = istr

    """
    match = regex.match("([a-zA-Z0-9_.]*)\((.*)\)\.([a-zA-Z0-9_]*)\((.*)\)", s)
    class_path = match.group(1)
    init_kwargs_ = match.group(2)
    _init_kwargs = regex_kv_pairs(init_kwargs_)
    init_kwargs = {key: _eval(val) for key, val in _init_kwargs.items()}
    method_name = match.group(3)
    method_kwargs_ = match.group(4)
    _method_kwargs = regex_kv_pairs(method_kwargs_)
    method_kwargs = {key: _eval(val) for key, val in _method_kwargs.items()}
    
    class_parts = class_path.split('.')
    class_name = class_parts[-1]
    module_name = '.'.join(class_parts[:-1])
    
    import_mod(module_name)
    mod = import_module(module_name)
   
    cls = getattr(mod, class_name)
    obj = cls(**init_kwargs)
    mth = getattr(obj, method_name)
    r = mth(**method_kwargs)
    if isinstance(r, Request):
        _ = r.compute()
    else:
        _ = r
    """
    if debug:
        print(f"dbx: exec: {s}")
    _ = _eval(s)
    return _    

