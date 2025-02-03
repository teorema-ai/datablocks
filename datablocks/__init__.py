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

_eval_ = __builtins__['eval']


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


def import_mod(path, globals, locals):
    path_parts = path.split('.')
    for i in range(1, len(path_parts)+1):
        _path = '.'.join(path_parts[:i])
        module = __import__(_path, globals, locals)
        globals[_path] = module


def exec(argstr=None, *, debug=False):
    import datablocks.dbx
    
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

    istr = argstr.strip()
    if istr.startswith('['):
        imports_start = 1
        imports_end = istr.find(']')
        if imports_end == -1:
            raise ValueError(f"Malformed imports specification: {istr}")
        imports = istr[imports_start:imports_end]
        module_names = [i.strip() for i in imports.split(',')]
        for module_name in module_names:
            import_mod(module_name, globals(), locals())
        s = istr[imports_end+1:].strip()
    else:
        s = istr

    if debug:
        print(f"dbx: exec: {s}")
    _ = _eval_(s)
    return _    

