import inspect


def func_signature(func):
    signature = func.signature if  hasattr(func, 'signature') else inspect.signature(func)
    return signature


def func_defaults(func):
    defaults = func.__defaults__ if hasattr(func, '__defaults__') else \
                    func.__call__.__defaults__ if (hasattr(func, '__call__') and hasattr(func.__call__, '__defaults__')) else \
                    []
    return defaults if defaults is not None else []


def func_kwdefaults(func):
    kwdefaults = func.__kwdefaults__ if hasattr(func, '__kwdefaults__') else \
                        func.__call__.__kwdefaults__ if (hasattr(func, '__call__') and hasattr(func.__call__, '__kwdefaults__')) else \
                        {}
    return kwdefaults if kwdefaults is not None else {}


def func_parameters(func, *, include=None, exclude=[]):
    if include is not None:
        pars = include
    else:
        sig = func_signature(func)
        pars = [par for par in sig.parameters if par not in exclude]
    return pars


def func_positional_parameters(func):
    sig = func_signature(func)
    kinds = [inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD]
    parnames = [name for name, par in sig.parameters.items() if par.kind in kinds]
    return parnames


def func_kwonly_parameters(func):
    sig = func_signature(func)
    kinds = [inspect.Parameter.KEYWORD_ONLY]
    parnames = [name for name, par in sig.parameters.items() if par.kind in kinds]
    return parnames


def ctor_name(cls):
        name = f"{cls.__module__}.{cls.__qualname__}"
        return name


class Signature:
    def __init__(self, args_schema=(), kwargs_schema=()):
        self.schema = args_schema, kwargs_schema


import copy
import inspect
import logging

from .signature import func_signature, func_defaults, func_kwdefaults, func_parameters, func_positional_parameters
from .utils import renumerate


logger = logging.getLogger(__name__)


# TODO: remove?
def func_kdefaults(func):
    # TODO: argnames are currently the same as pargs and iargnames is merely an enumeration of argnames
    argnames = func_positional_parameters(func)
    pargs = func_positional_parameters(func)
    iargnames = {a: i for i, a in enumerate(pargs) if a in argnames}
    dargs = {a: func.__defaults__[i] for a, i in iargnames.items()}
    return dargs


def func_iargs_kargs_kwargs(func, is_method, add_defaults, *args, **kwargs):
    """Separates args and kwargs into  according to aparams and kwparams, including  variable parameters, and optionally adds defaults.
    The result is a triple of dictionaries:
        _iargs: contains all positionals from args, kwargs and var_positionals indexed by argument position; ordered in ascending order of positions
        _kargs: contains all positionals from args, kwargs and var_keyword indexed by parameter name; ordered in ascending order of positions
        _kwargs: containing those kwargs not already in _kargs
    In _iargs and _kargs arguments from kwargs overwrite those from args, if both are present. # TODO: make this optional?
    All dictionaries are supplemented by the default arguments, if requested.
    """
    sig = func_signature(func)
    parameters_ = [name for name in sig.parameters.keys()]
    positionals_ = [name for name, p in sig.parameters.items()
                            if p.kind in [inspect.Parameter.POSITIONAL_ONLY,
                                                inspect.Parameter.POSITIONAL_OR_KEYWORD]]
    # FIX: 'self' seems to be removed from sig.parameters
    if is_method:
        parameters = parameters_[1:]
        positionals = positionals_[1:]
    else:
        parameters = parameters_
        positionals = positionals_
    position_keys = {i: k for i, k in enumerate(parameters)}
    key_positions = renumerate(parameters)
    defaults = func_defaults(func)
    kwdefaults = func_kwdefaults(func)

    non_default_aparams = positionals[:-len(defaults)]
    num_non_default_aparams = len(non_default_aparams)
    have_var_positional = inspect.Parameter.VAR_POSITIONAL in (p.kind for p in sig.parameters.values())
    have_var_keyword = inspect.Parameter.VAR_KEYWORD in (p.kind for p in sig.parameters.values())
    __iargs_ = {i: a for i, a in enumerate(args)}
    __iargtail_ = {}
    __ikeys = []
    __kwargtail = {}
    for k, v in kwargs.items():
        if k not in key_positions and not have_var_keyword:
            raise ValueError(f"kw argument with key {repr(k)} "
                             f"not in arg positions {key_positions} for func {func}")
        if k in key_positions:
            i = key_positions[k]
            if i < len(positionals):
                __iargtail_[i] = v
                __ikeys.append(k)
        else:
            __kwargtail[k] = v
    if add_defaults:
        __iargs = {i+num_non_default_aparams: defaults[i] for i in range(len(defaults))}
    else:
        __iargs = {}
    __iargs.update(__iargs_)
    __iargs.update(__iargtail_)
    if add_defaults:
        if len(__iargs) < len(positionals):
            raise ValueError(f"Supplied positional arguments {list(__iargs.values())} are insufficient "
                                         f"for positional parameters {positionals} for function {Tagger.func_tag(func)}")
        if len(__iargs) > len(positionals) and not have_var_positional:
            raise ValueError(f"Supplied positional arguments {list(__iargs.values())} are too numerous "
                             f"for positional parameters {positionals} for function {Tagger.func_tag(func)}")
    else:
        if len(__iargs) < num_non_default_aparams:
            raise ValueError(
                f"Supplied positional arguments {__iargs} are insufficient  "
                f"for positional parameters {non_default_aparams} for function {Tagger.func_tag(func)}")
    indices = sorted([i for i in __iargs.keys()])
    _iargs = {i: __iargs[i] for i in indices}
    _kargs = {position_keys[i]: v for i, v in _iargs.items() if i < len(positionals)}

    if add_defaults:
        kwargs_ = copy.copy(kwdefaults)
    else:
        kwargs_ = {}
    _kwargs_ = {k: v for k, v in kwargs.items() if k not in __ikeys and k not in __kwargtail}
    kwargs_.update(_kwargs_)
    keysi = sorted([key_positions[k] for k in kwargs_.keys()])
    _kwargs = {**{position_keys[i]: kwargs_[position_keys[i]] for i in keysi}, **__kwargtail}
    return _iargs, _kargs, _kwargs


def tag(o):
    if hasattr(o, '__tag__'):
        tag = o.__tag__()
    elif hasattr(o, '_tag_'):
        if callable(o._tag_):
            tag = o._tag_()
        elif isinstance(o._tag_, str):
            tag = o._tag_
        else:
            raise ValueError(f"'_tag_' {o._tag_} is neither 'str' nor callable")
    else:
        tag = repr(o)
    return tag


# TODO: tag_args --> tag_positional, tag_kwargs --> tag_keyword
# TODO: kwargs --> kvargs;, args mean positonal-or-keyword and kvargs mean keyword-only
class Tagger:
    def __init__(self, *, tag_args=True, tag_kwargs=True, tag_defaults=True):
        self.tag_args = tag_args
        self.tag_kwargs = tag_kwargs
        self.tag_defaults = tag_defaults

    # ltype: `str`|`repr`|`tag` 
    @staticmethod
    def func_name(ltype, func):
        if hasattr(func, '__qualname__'):
            ftag = func.__module__ + '.' + func.__qualname__
        elif hasattr(func, '__name__'):
            ftag = func.__module__ + '.' + func.__name__
        else:
            ftag = ltype(func)
        return ftag

    def func_tag(func):
        return Tagger.func_name(tag, func)

    @staticmethod
    def ctor_name(cls):
        name = f"{cls.__module__}.{cls.__qualname__}"
        return name

    @staticmethod
    def _static_label_func_args_kwargs(func, method, ltype, tag_args, tag_kwargs, tag_defaults, strict_args_kwargs, *args, **kwargs):
        if not isinstance(func, str):
            iargs_, kargs_, kvargs_ = func_iargs_kargs_kwargs(func, method, tag_defaults, *args, **kwargs)
            args_ = list(iargs_.values())
            kwargs_ = kvargs_
            if hasattr(func, 'signature'):
                signature = func.signature
            else:
                signature = inspect.signature(func)
            parameters = signature.parameters
            aparams = [k for k, p in parameters.items()
                    if p.kind == inspect.Parameter.POSITIONAL_ONLY or
                        p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD]
            if method:
                aparams = aparams[1:]
            kwparams = [k for k, p in parameters.items()
                        if p.kind == inspect.Parameter.KEYWORD_ONLY]
            if tag_defaults:
                _aparams = aparams
                _args = args_
                _kwparams = kwparams
                _kwargs = kwargs_
            else:
                _aparams = aparams[:len(args_)]
                _args = args_
                kwdefaults = func.__kwdefaults__ if hasattr(func, '__kwdefaults__') else \
                                    func.__call__.__kwdefaults__ if hasattr(func, '__call__') and hasattr(func.__call__,
                                                                                        '__kwdefaults__') else \
                                    None
                def default_kwarg(k):
                    return kwdefaults is not None and\
                                k in kwdefaults and\
                                            (k not in kwargs_ or \
                                            kwargs_[k] == kwdefaults[k])
                _kwparams = [k for k in kwparams if not default_kwarg(k)]
                _kwargs = {k: v for k, v in kwargs_.items() if not default_kwarg(k)}
            have_var_positional = inspect.Parameter.VAR_POSITIONAL in (p.kind for p in parameters.values())
            if strict_args_kwargs:
                if len(_args) < len(_aparams):
                    raise ValueError(f"Positional arguments {_args} are insufficient "
                                                f"for positional parameters {aparams} for function {Tagger.func_tag(func)}")
                if len(_args) > len(_aparams) and not have_var_positional:
                    raise ValueError(f"Positional arguments {_args} are excessive "
                                                f"for the number of parameters {aparams} for function {Tagger.func_tag(func)}")
                have_var_keyword = inspect.Parameter.VAR_KEYWORD in (p.kind for p in parameters.values())
                if len(_kwargs) < len(_kwparams):
                    raise ValueError(f"Keyword arguments {_kwargs} are insufficient "
                                                f"for keyword parameters {kwparams} for function {Tagger.func_tag(func)}")
                if len(_kwargs) > len(_kwparams) and not have_var_keyword:
                    raise ValueError(f"Keyword arguments {_kwargs} are excessive "
                                    f"for keyword parameters {kwparams}")
            targs, tkwargs = (), {}
            if tag_args and tag_kwargs:
                targs, tkwargs = Tagger._static_label_args_kwargs(ltype, tag_args, tag_kwargs, tag_defaults, *_args, **_kwargs)
            else:
                if tag_args:
                    targs, _ = Tagger._static_label_args_kwargs(ltype, tag_args, tag_kwargs, tag_defaults, *_args)
                if tag_kwargs:
                    _, tkwargs = Tagger._static_label_args_kwargs(ltype, tag_args, tag_kwargs, tag_defaults, **_kwargs)
        else:
            targs, tkwargs = Tagger._static_label_args_kwargs(ltype, tag_args, tag_kwargs, tag_defaults, *args, **kwargs)
        atag = ''
        if len(targs) > 0:
            atag = ", ".join(targs)
        kwtag = ''
        if len(tkwargs) > 0:
            kwtag = ", ".join(f"{k}={v}" for k, v in tkwargs.items())
        _tag = atag + ', ' + kwtag if len(atag) > 0 and len(kwtag) > 0 else atag + kwtag
        return _tag

    @staticmethod
    def _static_label_args_kwargs(ltype, tag_args, tag_kwargs, tag_defaults, *args, **kwargs):
        _args = [Tagger.static_label_object(ltype, tag_args, tag_kwargs, tag_defaults, v) for v in args]
        _kwargs = {k: Tagger.static_label_object(ltype, tag_args, tag_kwargs, tag_defaults, v) for k, v in kwargs.items()}
        return _args, _kwargs

    @staticmethod
    def static_label_object(ltype, tag_args, tag_kwargs, tag_defaults, arg):
        """Special arguments (e.g., functions) can be treated in a special way here, if necessary, rather than in individual object's repr"""
        if inspect.ismethod(arg):
            otag = Tagger.static_label_object(ltype, tag_args, tag_kwargs, tag_defaults, arg.__self__)
            label = f"{otag}.{arg.__func__.__name__}"
        elif callable(arg):
            label = Tagger.func_name(ltype, arg)
        elif isinstance(arg, str):
            label = repr(arg)
        else:
            label = ltype(arg)
        return label

    def label_object(self, ltype, arg):
        tag = Tagger.static_label_object(ltype, self.tag_args, self.tag_kwargs, self.tag_defaults, arg)
        return tag

    def tag_object(self, arg):
        _tag = Tagger.static_label_object(tag, self.tag_args, self.tag_kwargs, self.tag_defaults, arg)
        return _tag

    def repr_object(self, arg):
        tag = Tagger.static_label_object(repr, self.tag_args, self.tag_kwargs, self.tag_defaults, arg)
        return tag

    def str_object(self, arg):
        tag = Tagger.static_label_object(str, self.tag_args, self.tag_kwargs, self.tag_defaults, arg)
        return tag

    def label_args_kwargs(self, ltype, strict_args_kwargs, *args, **kwargs):
        tag = Tagger._static_label_args_kwargs(ltype, self.tag_args, self.tag_kwargs, self.tag_defaults, strict_args_kwargs, *args, **kwargs)
        return tag

    def tag_args_kwargs(self, *args, **kwargs):
        _tag = Tagger._static_label_args_kwargs(tag, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return _tag

    def repr_args_kwargs(self, *args, **kwargs):
        tag = Tagger._static_label_args_kwargs(repr, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return tag

    def str_args_kwargs(self, *args, **kwargs):
        tag = Tagger._static_label_args_kwargs(str, self.tag_args, self.tag_kwargs, self.tag_defaults,  *args, **kwargs)
        return tag
    
    @staticmethod
    def static_label_func_args_kwargs(func, ltype, tag_args, tag_kwargs, tag_defaults, *args, **kwargs):
        return Tagger._static_label_func_args_kwargs(func, False, ltype, tag_args, tag_kwargs, tag_defaults, *args, **kwargs)

    def label_func_args_kwargs(self, func, ltype, strict_args_kwargs, *args, **kwargs):
        tag = Tagger._static_label_func_args_kwargs(func, False, ltype, self.tag_args, self.tag_kwargs, self.tag_defaults, strict_args_kwargs, *args, **kwargs)
        return tag

    def tag_func_args_kwargs(self, func, *args, **kwargs):
        _tag = Tagger._static_label_func_args_kwargs(func, False, tag, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return _tag

    def repr_func_args_kwargs(self, func, *args, **kwargs):
        tag = Tagger._static_label_func_args_kwargs(func, False, repr, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return tag

    def str_func_args_kwargs(self, func, *args, **kwargs):
        tag = Tagger._static_label_func_args_kwargs(func, False, str, self.tag_args, self.tag_kwargs, self.tag_defaults,  *args, **kwargs)
        return tag

    @staticmethod
    def static_label_ctor(cls, ltype, tag_args, tag_kwargs, tag_defaults, strict_args_kwargs, *args, **kwargs):
        tag = f"{Tagger.ctor_name(cls)}({Tagger._static_label_func_args_kwargs(cls.__init__, True, ltype, tag_args, tag_kwargs, tag_defaults, strict_args_kwargs, *args, **kwargs)})"
        return tag

    def label_ctor(self, cls, ltype, strict_args_kwargs, *args, **kwargs):
        tag = Tagger.static_label_ctor(cls, ltype, self.tag_args, self.tag_kwargs, self.tag_defaults, strict_args_kwargs, *args, **kwargs)
        return tag

    def repr_ctor(self, cls, *args, **kwargs):
        strict_args_kwargs = True
        tag = Tagger.static_label_ctor(cls, repr, self.tag_args, self.tag_kwargs, self.tag_defaults, strict_args_kwargs, *args, **kwargs)
        return tag

    def tag_ctor(self, cls, *args, **kwargs):
        strict_args_kwargs = False
        _tag = Tagger.static_label_ctor(cls, tag, self.tag_args, self.tag_kwargs, self.tag_defaults, strict_args_kwargs, *args, **kwargs)
        return _tag

    def str_ctor(self, cls, *args, **kwargs):
        strict_args_kwargs = False
        tag = Tagger.static_label_ctor(cls, str, self.tag_args, self.tag_kwargs, self.tag_defaults, strict_args_kwargs, *args, **kwargs)
        return tag

    @staticmethod
    def static_label_new(cls, ltype, tag_args, tag_kwargs, tag_defaults, use_arg_tag_attr, *args, **kwargs):
        tag = f"{Tagger.ctor_name(cls)}({Tagger._static_label_func_args_kwargs(cls.__new__, True, ltype, tag_args, tag_kwargs, tag_defaults,  *args, **kwargs)})"
        return tag

    def label_new(self, cls, ltype, *args, **kwargs):
        tag = Tagger.static_label_new(cls, ltype, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return tag

    def tag_new(self, cls, *args, **kwargs):
        _tag = Tagger.static_label_new(cls, tag, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return _tag

    def repr_new(self, cls, *args, **kwargs):
        tag = Tagger.static_label_new(cls, repr, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return tag

    def str_new(self, cls, *args, **kwargs):
        tag = Tagger.static_label_new(cls, str, self.tag_args, self.tag_kwargs, self.tag_defaults, *args, **kwargs)
        return tag

    def label_func(self, func,  ltype, strict_args_kwargs, *args, **kwargs):
        if not isinstance(func, str):
            ftag = Tagger.static_label_object(ltype, self.tag_args, self.tag_kwargs, self.tag_defaults, func)
        else:
            ftag = func
        atag = self.label_func_args_kwargs(func, ltype, strict_args_kwargs, *args, **kwargs)
        tag = f"{ftag}({atag})"
        return tag

    def tag_func(self, func,  *args, **kwargs):
        strict_args_kwargs = False
        _tag = self.label_func(func, tag, strict_args_kwargs, *args, **kwargs)
        return _tag

    def repr_func(self, func,  *args, **kwargs):
        strict_args_kwargs = True
        _repr = self.label_func(func, repr, strict_args_kwargs, *args, **kwargs)
        return _repr

    def str_func(self, func,  *args, **kwargs):
        strict_args_kwargs = False
        _repr = self.label_func(func, str, strict_args_kwargs, *args, **kwargs)
        return _repr

    def tag_element(self, e):
        if isinstance(e, dict):
            te = self.tag_dict(e)
        elif isinstance(e, list):
            te = self.tag_list(e)
        elif hasattr(e, '__tag__'):
            te = e.__tag__()
        else:
            te = e
        return te

    def tag_dict(self, scope):
        tscope = {k: self.tag_element(v) for k, v in scope.items()}
        return tscope
            
    def tag_list(self, lst):
        tlst = [self.tag_element(e) for e in lst]
        return tlst

# ALIAS
Labeler = Tagger

