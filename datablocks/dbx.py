from collections.abc import Iterable
import copy
import dataclasses
from dataclasses import dataclass, asdict, replace, field
import functools
import hashlib
import importlib
from importlib import reload
import logging
import os
import pdb
import pickle
from typing import TypeVar, Generic, Tuple, Union, Dict, Optional, List

import fsspec


import pyarrow.parquet as pq
import pandas as pd


from . import signature
from .signature import (
    Signature, 
    ctor_name,
    tag, 
    Tagger,
)
from .utils import (
    OVERRIDE, 
    optional, 
    serializable, 
    microseconds_since_epoch, 
    datetime_to_microsecond_str,
    bash_eval,
    bash_expand,
)
from .eval import request, pool
from .eval.request import Request, ALL, LAST, NONE, Graph
from .eval.pool import (
    DATABLOCKS_STDOUT_LOGGING_POOL as STDOUT_POOL, 
    DATABLOCKS_STDOUT_LOGGING_POOL as STDOUT_LOGGING_POOL,
    DATABLOCKS_FILE_LOGGING_POOL as FILE_POOL,
    DATABLOCKS_FILE_LOGGING_POOL as FILE_LOGGING_POOL,
    DATABLOCKS_STDOUT_RAY_POOL as STDOUT_RAY_POOL, 
    DATABLOCKS_FILE_RAY_POOL as FILE_RAY_POOL,
)
from .dataspace import (
    DATABLOCKS_DATALAKE as DATALAKE,
    DATABLOCKS_HOMELAKE as HOMELAKE,
)
from .databuilder import Databuilder, UnknownTopic, DEFAULT_TOPIC

from .utils import gitrepostate, OVERRIDEN, sprint, pprint


class DEFAULT_ARG:
    pass


DEFAULT = DEFAULT_ARG


class DBXEvalError(Exception):
    pass


def dbx_eval(string):
    """
        A version of __builtins__.eval() in the context of imported datablocks.datablock.
    """
    import datablocks.dbx
    _eval_ = __builtins__['eval']
    try:
        _ = _eval_(string)
    except Exception as e:
        raise DBXEvalError(string) from e
    return _


_print = __builtins__['print']


logger = logging.getLogger(__name__)


HOME = os.environ['HOME']

T = TypeVar('T')
class RANGE(Generic[T], tuple):
    def __init__(self, *args):
        tuple.__init__(args)
        
    
DBX_PREFIX = 'DBX'


class Datablock:
    """
        Optional members:
        ```
            class RANGE(tuple):
                singular: str = 'element'
                ...

            def check_read_result(self, scope, *, result):
                '''
                    Validate if result could have been produced by build.
                '''
            
            def check_extent_scopes(self, scope, extent_scopes):
                '''
                    Validate if result could have been produced by build.
                '''

            def valid(self, blockscope, blockroots[, topic]) -> bool|list[bool]:
                '''
                    For a shardscope (blockscope with no ranges), return bool, indicating whether shard is valid.
                    For a propery blockscope (with ranges), return a list of bool, indicating whether each shard
                    within the block is valid.
                '''

            @staticmethod
            def summary(result):
                # Guard for None result, since summary may be applied at early stages of Request evaluation.
                if result is not None:
                    return result
        ```

    """
    REVISION = '0.0.1'
    TOPIC = None # replace this or define TOPICS: dict: topic -> filename

    @dataclass
    class SCOPE:
        ...

    def __init__(self, 
                 filesystem:fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
                 *, 
                 verbose=False, 
                 debug=False, ):
        self.filesystem = filesystem
        self.verbose = verbose
        self.debug = debug
        if hasattr(self, '__postinit__'):
            return self.__postinit__()

    @classmethod
    def range(cls):
        rangecls = RANGE if not hasattr(cls.SCOPE, 'RANGE') else cls.SCOPE.RANGE
        SCOPE_fields = dataclasses.fields(cls.SCOPE)
        has_ranges_dict = {field: isinstance(field.type, rangecls) for field in SCOPE_fields}
        has_ranges = any(has_ranges_dict.values())
        if not has_ranges:
            rangecls = None
        return rangecls

    def build(self, blockscope, blockroots):
        raise NotImplementedError()

    def read(self, blockscope, blockroots, topic=None):
        raise NotImplementedError()

    def summary(self, result):
        '''
            Ensure that build() returns None or something serializable, 
            if no summary() is defined.  Otherwise, post mortem analysis
            via show_build_graph() etc. will be highly difficult or 
            impossible.
        '''
        raise NotImplemented()

    def valid(self, blockscope, blockroots, topic=None):
        if self.range() is not None:
            raise ValueError("This basic version of 'valid()' can only handle shardscopes")
        if hasattr(self, 'TOPICS'):
            path = self.path(blockscope, blockroots, topic)
            _ = self.filesystem.exists(path)
        else:
            path = self.path(blockscope, blockroots)
            _ = self.filesystem.exists(path)
        return _            
    
    def metric(self, blockscope, blockroots, topic=None):
        def valid_to_metric(valid):
            if self.range() is None:
                metric = int(valid)
            else:
                metric = [int(v) for v in valid]
            return metric
        if topic is None:
            metric = valid_to_metric(self.valid(blockscope, blockroots))
        else:
            metric = valid_to_metric(self.valid(blockscope, blockroots, topic))
        return metric

    def path(self, scope_unused, shardroots, topic=None):
        if topic is not None:
            if shardroots is None:
                path = os.path.join(os.getcwd(), self.TOPICS[topic])
            else:
                path = os.path.join(shardroots[topic], self.TOPICS[topic])
        else:
            shardroot = shardroots
            if shardroot is None:
                path = os.join(os.getcwd(), self.TOPIC)
            else:
                path = os.path.join(shardroot, self.TOPIC)
        return path

    def paths(self, blockscope, blockroots, topic=None):
        '''
            Assume or check that there are as many blockroots as there are shards in blockscope,
            then form [self.path(None, blockroot[, topic]) for blockroot in blockroots].
        '''
        raise NotImplementedError

    def print_verbose(self, s):
        if self.verbose:
            print(f">>> {self.__class__.__qualname__}: {s}")

    def print_debug(self, s):
        if self.debug:
            print(f"DEBUG: >>> {self.__class__.__qualname__}: {s}")


@serializable
class DBX:
    """
        DBX instantiates a Databuilder class on demand, in particular, different instances depending on the build(**kwargs),
            . pool=Ray()
            . etc
        Not inheriting from Databuilder also has the advantage of hiding the varable scope API that Databuilder.build/read/etc presents.
        DBX is by definition a fixed scope block.
    """

    RECORD_SCHEMA_REVISION = '0.3.0'

    class Path(str):
        def __tag__(self):
            return bash_expand(self).replace('/', '|')
        def __str__(self):
            return bash_expand(self).replace(':', '/').replace('|', '/')
        def __repr__(self):
            s = expland_str(self).replace('|', '/').replace(':', '/')
            return f"datablocks.dbx.DBX.Path({repr(s)})"
            
    class ProxyRequest(request.Proxy):
        @staticmethod
        def parse_url(url: str, **datablock_kwargs):
            #url = {datablock.path.to.cls}@{revision}#{alias}:[{topic}]
            tail = url
            try:
                datablock_clstr, tail  = tail.split("@")
            except:
                raise ValueError(f"Malformed url: {url}")
            try:
                revision_alias, topic   = tail.split(":")
                if revision_alias.find('#') == -1:
                    raise ValueError(f"Cannot forward a proxy request to a DBX without alias")
                else:
                    revision, alias = revision_alias.split('#')
                if len(revision) == 0:
                    raise ValueError(f"Malformed ProxyRequest url: no revision: {url}")
                if len(alias) == 0:
                    raise ValueError(f"Malformed ProxyRequest url: no alias: {url}")
            except:
                raise ValueError(f"Malformed url: {url}")
            if len(topic) == 0:
                topic = DEFAULT_TOPIC
            dbx = DBX(datablock_clstr, alias, revision=revision, **datablock_kwargs)
            return dbx, topic

        def __init__(self, kind: str, spec: Union[str, tuple], pic=False):
            """
                kind: 'READ' | 'PATH'
                spec: url | (dbx, topic)
            """
            #pdb.set_trace()
            self.kind = kind
            self.spec = spec
            if isinstance(self.spec, tuple):
                self.dbx, self.topic = spec
            else:
                self.dbx, self.topic = self.parse_url(spec)
            self.pic = pic
            self.dbx = self.dbx.with_pic(pic=pic)

        @property
        def request(self):
            if self.kind == 'READ':
                request = self.dbx.read_request(topic=self.topic)
            elif self.kind == 'PATH':
                request = self.dbx.path_request(topic=self.topic)
            else:
                raise ValueError(f"Unknown ProxyRequest kind: '{self.kind}'")
            return request

        def with_pic(self, pic=True):
            _ = self.__class__(self.spec, pic=pic)
            return _

        def __ne__(self, other):
            # .request might fail due to previous build record mismatch, 
            # so treat that as a failure of equality
            _ =  not isinstance(other, self.__class__) or \
                repr(self.request) != repr(other.request) #TODO: implement a more principled Request __ne__
            return _
        
        def __eq__(self, other):
            return not self.__ne__(other)
        
        def __str__(self):
            _ = self.__repr__()
            return _ 
        
        def __repr__(self):
            argstr = f"('{self.topic}')" if self.topic is not None else "()"
            _ = repr(self.dbx) + \
                f".{self.kind}{argstr}"
            return _

        def __tag__(self):
            tag_ = f"{DBX_PREFIX}.{self.dbx._datablock_clstr}@{self.dbx.databuilder.revision}#{self.dbx.databuilder.alias}"
            tag = tag_ + f":{self.topic}" if self.topic != DEFAULT_TOPIC else tag_+""
            return tag
        
    class ReadRequest(ProxyRequest):
        def __init__(str, spec: Union[str, tuple], pic=False):
            super().__init__('READ', spec, pic=pic)

    class PathRequest(ProxyRequest):
        def __init__(str, spec: Union[str, tuple], pic=False):
            super().__init__('PATH', spec, pic=pic)

    def show_exec_aliases(self):
        records = self.show_exec_records()
        #! dedup and return record aliases
        return records

    @staticmethod
    def show_datablocks(*, dataspace=DATALAKE, pretty_print=True, debug=False):
        def _chase_anchors(_dataspace, _anchorchain=()):
            pathnames = [path for path in _dataspace.list() if _dataspace.isdir(path, relative=True)]
            if debug:
                print(f"DEBUG: _chase_anchors: pathnames: {pathnames}")
            anchorchains = []
            for pathname in pathnames:
                if debug:
                    print(f"DEBUG: _chase_anchors: processing pathname: '{pathname}'")
                if pathname.startswith('.'):
                    if debug:
                        print(f"DEBUG: _chase_anchors: skipping '{pathname}': starts with '.'")
                    continue
                elif _dataspace.isfile(pathname, relative=True):
                    raise ValueError(f"_chase_anchors: encountered a file '{pathname}'. Run with debug=True to investigate.")
                elif pathname.startswith('@'):
                    anchorchain = _anchorchain + (pathname,)
                    if debug:
                        print(f"DEBUG: show_datablocks(): chase_anchors(): adding anchorchain {anchorchain} to the collection after reaching a terminal pathname {pathname}")
                    anchorchains.append(anchorchain)
                else:
                    dataspace_ = _dataspace.subspace(pathname)
                    anchorchain_ = _anchorchain+(pathname,)
                    if debug:
                        print(f"DEBUG: _chase_anchors: recursive call into dataspace_ {dataspace_}")
                    anchorchains_ = _chase_anchors(dataspace_, anchorchain_)
                    anchorchains.extend(anchorchains_)
            if debug:
                    print(f"DEBUG: _chase_anchors: returning: reached dataspace with no subdirectories: {_dataspace}")
            return anchorchains
        
        datablock_dataspace = dataspace.subspace(DBX_PREFIX).ensure()
        if debug:
            print(f"DEBUG: chasing anchors in datablock_dataspace: {datablock_dataspace}")
        anchorchains = _chase_anchors(datablock_dataspace)
        if debug:
            print(f"DEBUG: show_datablocks(): anchorchains: {anchorchains}")

        _datablocks = {}
        for anchorchain in anchorchains:
            modpath = '.'.join(anchorchain[:-1])
            revision = str(DBX.Path(anchorchain[-1]))
            if modpath in _datablocks:
                _datablocks[modpath].append(revision)
            else:
                _datablocks[modpath] = [revision]
        datablocks = {key: list(set(val)) for key, val in _datablocks.items()}
        if pretty_print:
                pprint(datablocks)
        else:
            return datablocks
    @dataclass
    class State:
        spec: Union[Datablock, str]
        alias: Optional[str] = None
        pic: bool = False
        repo: Optional[str] = None
        revision: Optional[str] = None
        verbose: Optional[str] = False
        debug: bool = False
        databuilder_kwargs_: Optional[Dict] = None
        datablock_kwargs_: Optional[Dict] = None
        datablock_scope_kwargs_: Optional[Dict] = None

        def __postinit__(self):
            if self.databuilder_kwargs_ is None:
                self.databuilder_kwargs_ = {}
            if self.datablock_kwargs_ is None:
                self.datablock_kwargs_ = {}
            if self.datablock_scope_kwargs_ is None:
                self.datablock_scope_kwargs_ = {}

    @staticmethod
    def scopes_equal(s1, s2):
        # TODO: consistency check: sha256 alias must be unique for a given revision or match scope
        if set(s1.keys()) != (s2.keys()):
            return False
        for key in s1.keys():
            if s1[key] != s2[key]:
                return False
        return True

    def __init__(self, 
                spec: Optional[Union[type, str]] = None, #Datablock cls or clstr
                alias=None,
                *,
                pic=False,
                repo=None,
                revision=None,
                verbose=False,
                debug=False,  
    ):
        self.__setstate__(DBX.State(spec=spec,
                                alias=alias,
                                pic=pic,
                                repo=repo,
                                revision=revision,
                                verbose=verbose,
                                debug=debug
        ))
        
    def __getstate__(self):
        return DBX.State(spec=self.spec,
                         alias=self.alias,
                         pic=self.pic,
                         repo=self.repo,
                         revision=self.revision,
                         verbose=self.verbose,
                         debug=self.debug,
                         databuilder_kwargs_=self.databuilder_kwargs_,
                         datablock_kwargs_=self.datablock_kwargs_,
                         datablock_scope_kwargs_=self.datablock_scope_kwargs_,
        )

    def __setstate__(self, 
                     state: State, 
    ):
        for key, val in asdict(state).items():
            setattr(self, key, bash_expand(val))

        if self.datablock_kwargs_ is None:
            self.datablock_kwargs_ = {}
        def update_datablock_kwargs(**kwargs):
            self.datablock_kwargs_.update(**kwargs)
            return self
        self.Datablock = update_datablock_kwargs

        if self.datablock_scope_kwargs_ is None:
            self.datablock_scope_kwargs_ = {}
        for key, val in self.datablock_scope_kwargs_.items():
            if isinstance(val, DBX.ProxyRequest):
                self.datablock_scope_kwargs_[key] = val.with_pic(state.pic)
        def update_scope(**kwargs):
            _kwargs = {key: bash_expand(val) for key, val in kwargs.items()}
            self.datablock_scope_kwargs_.update(**_kwargs)
            return self
        self.SCOPE = update_scope

        if self.databuilder_kwargs_ is None:
            self.databuilder_kwargs_ = {}
        @functools.wraps(Databuilder)
        def update_databuilder_kwargs(**kwargs):
            self.databuilder_kwargs_.update(**kwargs)
            return self
        self.Databuilder = update_databuilder_kwargs

        self._scope = None
        self._datablock_module_name = None
        self._datablock_clstr = None
        self._datablock_clsname = None
        self._datablock_cls = None
        self._datablock = None
        self._databuilder = None

        if self.spec is not None:
            if isinstance(self.spec, str):
                self._datablock_clstr = self.spec
                datablock_clstrparts = self._datablock_clstr.split('.')
                if len(datablock_clstrparts) == 1:
                    self._datablock_module_name = __name__
                else:
                    self._datablock_module_name = '.'.join(datablock_clstrparts[:-1])
                self._datablock_clsname = datablock_clstrparts[-1]
            else:
                self._datablock_cls = self.spec
                self._datablock_clsname = self._datablock_cls.__qualname__
                self._datablock_module_name = self._datablock_cls.__module__
                self._datablock_clstr = f"{self._datablock_module_name}.{self._datablock_cls.__name__}"
        return self
    
    def DBX(self, 
              *,
              verbose=DEFAULT,
              debug=DEFAULT,  
              pic=DEFAULT,
              repo=DEFAULT,
              revision=DEFAULT,
    ):
        state = self.__getstate__()
        kwargs = {}
        if debug != DEFAULT:
            state.debug = debug
        if verbose != DEFAULT:
            kwargs['verbose'] = verbose
        if pic != DEFAULT:
            kwargs['pic'] = pic
        if repo != DEFAULT:
            kwargs['repo'] = repo
        if revision != DEFAULT:
            kwargs['revision'] = revision
        state = replace(state, **kwargs)

        clone = DBX().__setstate__(state)
        return clone

    def clone(self, *args, **kwargs):
        return self.DBX(*args, **kwargs)

    def with_pic(self, pic=True):
        _ = self.clone(pic=pic)
        return _

    def __repr__(self):
        args = (self.spec, self.alias) if self.alias else (self.spec,)
        kwargs = {}
        if self.repo is not None:
            kwargs['repo'] = self.repo
        if self.revision is not None:
            kwargs['revision'] = self.revision
        _ =  Tagger(tag_defaults=False).str_ctor(self.__class__, args, kwargs)
        return _

    def __tag__(self):
        return f"{DBX_PREFIX}.{self.spec}#{self.alias if self.alias is not None else ''}"

    def __hash__(self):
        #TODO think this through
        prefixstr = f"{self.__class__}--{self.spec}--{self.alias}"
        statestr = pickle.dumps(self.__getstate__())
        hashstr = prefixstr + statestr
        _ =  int(hashlib.sha1(hashstr.encode()).hexdigest(), 16)
        return _

    '''
    #TODO: #REMOVE?
    def __getattr__(self, attr):
        try:
            datablock_clsname = super().__getattribute__('_datablock_clsname')
            if attr == datablock_clsname:
                return self.Datablock
            else:
                super().__getattribute__(attr)
        except:
            return super().__getattribute__(attr)
    '''

    @property
    def anchorspace(self):
        return self.databuilder.anchorspace

    @property
    def TOPICS(self):
        if hasattr(self.datablock_cls, 'TOPICS'):
            return self.datablock_cls.TOPICS
        else:
            raise AttributeError(f"Class {self.datablock_cls} does not have attribute 'TOPICS'")

    @property
    def TOPIC(self):
        if hasattr(self.datablock_cls, 'TOPIC'):
            return self.datablock_cls.TOPIC
        else:
            raise AttributeError(f"Class {self.datablock_cls} does not have attribute 'TOPIC'")

    def reload(self):
        self._datablock_cls = None
        return self

    @property
    def datablock_cls(self):
        if self._datablock_cls is None:
            with gitrepostate(self.repo, self.revision, verbose=self.verbose):
                mod = importlib.import_module(self._datablock_module_name)
            self._datablock_cls = getattr(mod, self._datablock_clsname)
        return self._datablock_cls

    @property
    def databuilder(self):
        if self._databuilder is None:
            databuilder_cls = self.databuilder_cls()
            databuilder_kwargs = copy.copy(self.databuilder_kwargs_)
            if 'dataspace' in databuilder_kwargs:
                databuilder_kwargs['dataspace'] = databuilder_kwargs['dataspace'].with_pic(True) #TODO: why with_pick(True)?
            #DEBUG
            #pdb.set_trace()
            self._databuilder = databuilder_cls(self.alias, 
                                        build_block_request_lifecycle_callback=self._build_block_request_lifecycle_callback_,
                                        **databuilder_kwargs)
        return self._databuilder

    @property
    def datablock(self):
        if self._datablock is None:
            self._datablock = self.databuilder.datablock
        return self._datablock

    @property
    def scope_kwargs(self):
        return self.datablock_scope_kwargs_

    def show_exec_record_scope(self):
        scope = None
        if self.databuilder.alias is not None:
            record = self.show_named_exec_record(alias=self.databuilder.alias, revision=self.revision, stage='BUILD_END') 
            if record is not None:
                try:
                    scope = dbx_eval(record['scope'])
                    if self.verbose:
                        print(f"{self}: scope: obtained from records: {scope}")
                except DBXEvalError as ee:
                    raise ValueError(f"Failed to parse build scope {record['scope']}") from ee
        return scope

    def show_dataspace_batches(self):
        return self.databuilder.dataspace_batch_scopes()

    @property
    def scope(self) -> dict:
        """
            #TODO: DBX.datablock_scope -> DBX.SCOPE, DBX.SCOPE -> DBX.scope, DBX.scope -> DBX.SCOPE.asdict()
        """
        if self._scope is None:
            import datablocks #TODO: why the local import?
            
            scope = None
            record_scope = self.show_exec_record_scope()
            if len(self.scope_kwargs) > 0:
                scope = asdict(self.datablock_cls.SCOPE(**self.scope_kwargs))
                if self.verbose:
                    print(f"{self}: scope: constructed scope_kwargs:\n{self.scope_kwargs}")          
            elif record_scope is not None:
                scope = record_scope
                if self.verbose:
                    print(f"{self}: scope: using record scope: {scope}")
            else:
                scope = asdict(self.datablock_cls.SCOPE())
                if self.verbose:
                    print(f"{self}: scope: default scope: {scope}")
            scope = self.databuilder._blockscope_(**scope)
            if scope is not None and record_scope is not None and not self.scopes_equal(scope, record_scope):
                raise ValueError(f"Attempt to overwrite record scope {record_scope} with {scope} for {self.datablock_cls} alias {self.alias}")
            self._scope = scope
        return self._scope

    @property
    def datablock_scope(self):
        #TODO: make this primary and def scope() derived using asdict()
        return self.datablock_cls.SCOPE(**self.scope)
    
    @property
    def tagscope(self):
        _ = self.databuilder._tagscope_(**self.scope)
        return _

    @property
    def topics(self):
        if hasattr(self.datablock_cls, 'TOPICS'):
            if isinstance(self.datablock_cls.TOPICS, dict):
                topics = list(self.datablock_cls.TOPICS.keys())
            else:
                topics = self.datablock_cls.TOPICS
        else:
            topics = [DEFAULT_TOPIC]
        return topics

    __build_failure_doc__ = """Try running with .Datablock(throw=True) or examine the build using these methods:
    .show_exec_records()
    .show_exec_record()
    .show_build_graph()
    .show_build_batch_graph()
    .show_build_batch_graph().log()
    .show_build_batch_graph().traceback
    
    If the last build record has status 'STATUS.RUNNING', then the build failed and exception was not captured, indicating a DBX bug.
    In this case .Datablock(throw=True) or .Datablock(pool=FILE_LOGGING_POOL) followed by .show_build_batch_graph().log() may be the only useful debugging methods.
    """
    def build(self):
        """In case of failure: 
        """ + DBX.__build_failure_doc__
        try:
            request = self.build_request()
            response = request.evaluate() # need .evaluate() for its response rather than just .compute to examine status later
            _ = response.result() # extract result to force computation
        except UnknownTopic as e:
            raise ValueError(f"Unknown topic error: wrong/stale scope?") from e
        if self.verbose and not response.succeeded:
            print(f"Build failed.  {DBX.__build_failure_doc__}")
        return f"SUCCESS: {response.succeeded}"

    def build_request(self):
        request = self.databuilder.build_block_request(**self.scope)
        return request
    
    def path(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"{self} has not been built yet")
        tagscope = self.databuilder._tagscope_(**self.scope)
        page = self.databuilder._block_extent_page_(topic, **tagscope)
        if len(page) > 1:
            path = list(page.values())
        elif len(page) == 1:
            path = list(page.values())[0]
        else:
            path = None
        return path
    
    def path_request(self, topic=DEFAULT_TOPIC):
        _ = request.Request(self.path, topic)
        return _

    def read_request(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"'None' scope, perhaps {self} has not been built yet")
        #pdb.set_trace()
        request = self.databuilder.read_block_request(topic, **self.scope)\
            .set(summary=lambda _: self.extent[topic]) # TODO: #REMOVE?
        return request

    def PATH(self, topic=None):
        _ = DBX.PathRequest((self, topic), pic=self.pic)
        return _

    def READ(self, topic=None):
        #pdb.set_trace()
        _ = DBX.ReadRequest((self, topic), pic=self.pic)
        return _
    
    def read(self, topic=DEFAULT_TOPIC):
        if topic not in self.topics:
            raise ValueError(f"Unknown topic: '{topic}' not in {self.topics}")
        read_request = self.read_request(topic)
        result = read_request.compute()
        return result
    
    @property
    def intent(self):
        _ = self.databuilder.block_intent(**self.scope)
        return _

    @property
    def extent(self):
        _ = self.databuilder.block_extent(**self.scope)
        return _

    def extentls(self, topic=None):
        paths = self.databuilder.block_extent(**self.scope)
        if topic is not None:
            paths = paths[topic]
        if isinstance(paths, dict):
            raise ValueError(f"No topic selected")
        for path in paths:
            files = self.databuilder.dataspace.filesystem.ls(path)
            print(f"{path}:")
            for f in files:
                print(f"\t{f}")
    
    @property
    def shortfall(self):
        _ = self.databuilder.block_shortfall(**self.scope)
        return _
    
    @property
    def metric(self):
        _ = self.databuilder.block_metric(**self.scope)
        return _

    @property
    def valid(self):
        _ = self.extent
        return _

    def extent_scopes(self):
        tagscope = self.databuilder.Tagscope(**self.scope)
        return self.databuilder.extent_scope(**tagscope)

    def shortfall_scopes(self):
        tagscope = self.databuilder.Tagscope(**self.scope)
        return self.databuilder.shortfall_scope(**tagscope)

    def scope_roots(self, topic=None, **scope):
        tagscope = self.databuilder.Tagscope(**scope)
        roots = self.databuilder.datablock_blockroots(tagscope)
        if topic is not None:
            roots = roots[topic]
        return roots

    @property
    def roots(self):
        tagscope = self.databuilder.Tagscope(**self.scope)
        return self.databuilder.datablock_blockroots(tagscope)

    @property
    def check_extent_scopes(self,):
        if hasattr(self.datablock, 'check_extent_scopes'):
            def check_extent_scopes(scope, extent_scopes):
                return self.datablock.check_extent_scopes(scope, extent_scopes,)
            return check_extent_scopes

    @property
    def check_read_result(self,):
        # This could be done exclusively on the Datablock side, but we are putting all the boilerlate stuff here,
        # reducing the Datablock-specific code.
        if hasattr(self.datablock, 'check_read_result'):
            def check_read_result(topic=None, *, result):
                if hasattr(self.datablock, 'TOPICS'):
                    return self.datablock.check_read_result(self.datablock_scope, topic, result=result)
                else:
                    return self.datablock.check_read_result(self.datablock_scope, result=result)
            return check_read_result

    BUILD_RECORDS_COLUMNS_SHORT = ['stage', 'revision', 'scope', 'alias', 'task_id', 'metric', 'status', 'date', 'timestamp', 'runtime_secs']

    def show_exec_records(self, *, stage='ALL', full=False, columns=None, all=False, tail=5, debug: bool = False):
        """
        All build records for a given Databuilder class, irrespective of revision (see `show_exec_record()` for retrieval of more specific information).
        'all=True' forces 'tail=None'
        """
        short = not full
        recordspace = self._recordspace_()

        frame = None
        try:
            if debug:
                print(f"DEBUG: show_exec_records(): looking for records in recordspace {recordspace}")
            filepaths = [recordspace.join(recordspace.root, filename) for filename in recordspace.list()]
            
            frames = {}
            for filepath in filepaths:
                try:
                    frame = pd.read_parquet(filepath, storage_options=recordspace.filesystem.storage_options)
                except:
                    print(f"Skipping corrupted build record path {filepath}")
                frames[filepath] = frame
            frame = pd.concat(list(frames.values())) if len(frames) > 0 else pd.DataFrame()
            
        except FileNotFoundError as e:
            #TODO: ensure it is exactly recordspace.root that is missing
            pass
    
        if frame is not None and len(frame) > 0:
            frame.reset_index(inplace=True, drop=True)
            if columns is None:
                _columns = frame.columns
            else:
                _columns = [c for c in columns if c in frame.columns]
            if short:
                _columns_ = [c for c in _columns if c in DBX.BUILD_RECORDS_COLUMNS_SHORT]
            else:
                _columns_ = _columns
            frame.sort_values(['timestamp'], inplace=True)
            if stage is not None and stage != 'ALL':
                _frame = frame[frame.stage == stage][_columns_]
            else:
                _frame = frame[_columns_]
        else:
            _frame = pd.DataFrame()

        if all:
            tail = None
        if tail is not None:
            __frame = _frame.tail(tail)
        else:
            __frame = _frame
        
        if self.alias is not None:
            if 'alias' not in __frame:
                print(f"WARNING: no 'alias' in records dataframe (legacy records?).  Returning all records.")
                __frame_ = __frame
            else:
                __frame_ = __frame[__frame['alias'] == self.alias]
        else:
            __frame_ = __frame
        return __frame_

    def show_exec_record_columns(self, *, full=True, **ignored):
        frame = self.show_exec_records(full=full)
        columns = frame.columns
        return columns

    def show_exec_record(self, record=None, *, full=False, stage='ALL'):
        import datablocks
        try:
            records = self.show_exec_records(full=full, stage=stage)
        except:
            return None
        if len(records) == 0:
            return None
        if isinstance(record, int):
            _record = records.loc[record]
        elif record is None:
            _record = records.iloc[-1]
        else:
            _record = None
        return _record

    def show_named_exec_record(self, *, alias=None, revision=None, full=False, stage=None):
        import datablocks # needed for `eval`
        records = self.show_exec_records(full=full, stage=stage)
        if self.debug:
            print(f"show_name_record: databuilder {self}: revision: {repr(revision)}, alias: {repr(alias)}: retrieved records: len: {len(records)}:\n{records}")
        if len(records) == 0:
            return None
        if alias is not None:
            records0 = records.loc[records.alias == alias]
        else:
            records0 = records
        if revision is not None:
            if 'revision' in records0.columns:
                records1 = records0.loc[records0.revision == repr(revision)] #NB: must compare to string repr
            else:
                records1 = pd.DataFrame()
        else:
            records1 = records0
        if self.debug:
            print(f"show_name_record: filtered records1: len: {len(records1)}:\n{records1}")
        if len(records1) > 0:
            record = records1.iloc[-1]
        else:
            record = None
        return record

    def show_build_records(self, *, full=False, columns=None, all=False, tail=5):
        return self.show_exec_records(stage="BUILD_END", full=full, columns=columns, all=all, tail=tail)

    def show_build_record(self, record=None, *, full=False,):
        return self.show_exec_record(record, full=full, stage="BUILD_END")

    @staticmethod
    def exec_records(datablock_cls_or_str, *, full: bool = False, all: bool = False):
        dbx = DBX(datablock_cls_or_str)
        return dbx.show_exec_records(full=full, all=all,)

    @staticmethod
    def exec_scopes(datablock_cls_or_str, *, anonymous: bool = False) -> Union[Dict, List]:
        dbx = DBX(datablock_cls_or_str)
        records = dbx.show_build_records(full=True, all=True,)
        if not anonymous: 
            scopes = {
                self.exec_record_alias(record): self.exec_record_scope(record)
                for record in records
                if self.exec_record_alias(record) is not None
            }
        else:
            scopes = [
                self.exec_record_scope(record)
                for record in records
                if self.exec_record_alias(record) is None
            ]
        return scopes

    def show_exec_scopes(self, *, anonymous: bool = False):
        return self.exec_scopes(self.datablock_cls, anonymous=anonymous)

    def show_build_batch_graph(self, record=None, *, batch=0, **graph_kwargs):
        g = self.show_build_graph(record=record, node=(batch,), **graph_kwargs) # argument number `batch` to the outermost Request AND
        return g
    
    def show_build_batch_count(self, record=None):
        g = self.show_build_graph(record=record) 
        if g is None:
            nbatches = None
        else:
            nbatches = len(g.args)-1 # number of arguments less one to the outermost Request AND(batch_request[, batch_request, ...], extent_request)
        return nbatches
    
    def show_exec_transcript(self, record=None, **ignored):
        _record = self.show_exec_record(record=record, full=True)
        if _record is None:
            summary = None
        else:
            summary = _record['report_transcript']
        return summary

    @staticmethod
    def exec_record_scope(record):
        if record is not None:
            scopestr = record['scope']
            try:
                scope = dbx_eval(scopestr)
            except DBXEvalError as ee:
                raise ValueError(f"Failed to parse build scope {scopestr}") from ee
        else:
            scope = None
        return scope

    @staticmethod
    def exec_record_alias(record):
        return record['alias']

    def show_exec_scope(self, record=None, **ignored):
        _record = self.show_exec_record(record=record, full=True)
        return DBX._exec_record_scope(_record)

    def show_exec_graph(self, record=None, *, stage='ALL', node=tuple(), show_=('logpath', 'logpath_status', 'exception'), show=tuple(), **kwargs):
        if not isinstance(node, Iterable):
            node = (node,)
        _record = self.show_exec_record(record=record, full=True, stage=stage)
        if _record is None:
            if self.verbose:
                print(f"WARNING: show_exec_graph(): unable to retrieve an exec record, returning None")
            return None
        _transcript = _record['report_transcript']
        if _transcript is None:
            return None
        if isinstance(show_, str):
            show_=(show_,)
        if isinstance(show, str):
            show=(show,)
        show = show_ + show
        _graph = Graph(_transcript, show=show, **kwargs)
        if node is not None:
            graph = _graph.node(*node)
        else:
            graph = _graph
        return graph

    def show_build_graph(self, record=None, *, node=tuple(), show_=('logpath', 'logpath_status', 'exception'), show=tuple(), **kwargs):
        return show_exec_graph(record, node=node, stage='BUILD_END', show_=show_, show=tuple(), **kwargs)

    def UNSAFE_clear_exec_records(self):
        self._recordspace_().remove()
    
    def UNSAFE_clear_request(self):
        blockscope = self.databuilder._blockscope_(**self.scope)
        tagblockscope = self.databuilder._tagscope_(**blockscope)
        request = Request(self._UNSAFE_clear_block_, **tagblockscope)
        return request

    def UNSAFE_clear(self):
        request = self.UNSAFE_clear_request()
        _ = request.compute()
        return _

    @OVERRIDE
    def _UNSAFE_clear_block_(self, **scope):
        blockscope = self.databuilder._blockscope_(**scope)
        for topic in self.databuilder.topics:
            shardspace = self.databuilder._shardspace_(topic, **blockscope)
            if self.verbose:
                print(f"Clearing shardspace {shardspace}")
            shardspace.remove()

    def _validate_subscope(self, **subscope):
        #TODO: check that subscope is a subscope of self.scope
        return subscope

    def _recordspace_(self):
        subspace = self.databuilder.anchorspace.subspace('.records', f'schema={self.RECORD_SCHEMA_REVISION}')
        return subspace

    def register(self):
        #tagscope = self._tagscope_(**blockscope)
        classname = ctor_name(self.__class__)
        revisionstr = repr(self.revision)
        blockscope = self.scope
        namestr = f"{classname}:{revisionstr}(**{blockscope})"
        hashstr = hashlib.sha256(namestr.encode()).hexdigest()[:10]
        alias = self.alias
        if alias is None:
            alias = hashstr
        recordspace = self._recordspace_().ensure()

        task = None
        timestamp = int(microseconds_since_epoch())
        datestr = datetime_to_microsecond_str()
        blockscopestr = repr(blockscope)
        record = dict(schema=DBX.RECORD_SCHEMA_REVISION,
                           alias=self.alias,
                           stage="REGISTRATION",
                           classname=classname,
                           revision=revisionstr,
                           scope=blockscopestr,
                           date=datestr,
                           timestamp=timestamp,
                           runtime_secs=str(None),
                           cookie=str(None),
                           id=str(None),
                           logspace=str(None),
                           logname=str(None),
                           status=str(None),
                           success=str(None),
                           report_transcript=str(None),
        )
        
        if self.verbose:
            print(f"DBX REGISTRATION: writing registration record for DBX {self}")
            
        record_frame = pd.DataFrame.from_records([record])
        record_frame.index.name = 'index'
        record_filepath = \
            recordspace.join(recordspace.root, f"alias-{alias}-stage-REGISTRATION-task_id-None-datetime-{datestr}.parquet")
        record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)

    def _build_block_request_lifecycle_callback_(self, **blockscope):
        #tagscope = self._tagscope_(**blockscope)
        classname = ctor_name(self.__class__)
        revisionstr = repr(self.revision)
        #tagscopestr = repr(tagscope)
        namestr = f"{classname}:{revisionstr}(**{blockscope})"
        hashstr = hashlib.sha256(namestr.encode()).hexdigest()[:10]
        alias = self.alias
        if alias is None:
            alias = hashstr
        recordspace = self._recordspace_().ensure()

        def _write_record_lifecycle_callback(lifecycle_stage, request, response):
            # TODO: report_transcript should be just a repr(report.to_dict()), ideally, repr(report)
            # TODO: however, we may rewrite report.result
            task = request.task
            timestamp = int(microseconds_since_epoch())
            datestr = datetime_to_microsecond_str()
            blockscopestr = repr(blockscope)
            _record = dict(schema=DBX.RECORD_SCHEMA_REVISION,
                           alias=alias,
                           stage=f"BUILD_{lifecycle_stage.name}",
                           classname=classname,
                            revision=revisionstr,
                            scope=blockscopestr,
                            date=datestr,
                            timestamp=timestamp,
                            runtime_secs='',
                            cookie=str(task.cookie),
                            id=str(task.id),
                            logspace=str(task.logspace),
                            logname=str(task.logname),
                            status='',
                            success='',
                            report_transcript='',
            )
        
            if self.verbose:
                    print(f"DBX LIFECYCLE: {lifecycle_stage.name}: writing record for request:\n{request}")
                    print(f"DBX LIFECYCLE: {lifecycle_stage.name}: topics: {self.topics}")
                    print(f"DBX LIFECYCLE: {lifecycle_stage.name}: blockscope: {blockscope}")
            
            logname = None
            task_id = None
            if response is not None:
                if response.done_time is not None and response.start_time is not None:
                    runtime_secs = (response.done_time - response.start_time).total_seconds()
                else:
                    runtime_secs = str(None)
                report = response.report() # snapshot of report at this stage in lifecycle
                task_id = response.id
                report_transcript = report.transcript() 
                #TODO: #REMOVE
                #args_reports = report.args_reports
                #kwargs_reports = report.kwargs_reports
                #args_results = [arg_report.result if isinstance(arg_report, Report) else arg_report for arg_report in args_reports]
                #kwargs_results = {key: arg_report.result if isinstance(arg_report, Report) else arg_report for key, arg_report in kwargs_reports.items()}
                logspace=response.logspace
                logpath = report.logpath
                if logpath is not None:
                    _, logname_ext = os.path.split(logpath)
                    logname, ext = logname_ext.split('.')
                transcriptstr = repr(report_transcript)
                if self.debug:
                    print(f"DBX LIFECYCLE: {lifecycle_stage.name}: transcript: {transcriptstr}, logpath: {logpath}, logname: {logname}")
                _record.update(dict(
                                    task_id=str(task_id),
                                    runtime_secs=f"{runtime_secs}",
                                    status=report_transcript['status'],
                                    success=report_transcript['success'],
                                    logspace=repr(logspace),
                                    logname=logname,
                                    report_transcript=transcriptstr,
                            ))
            record_frame = pd.DataFrame.from_records([_record])
            record_frame.index.name = 'index'
            record_filepath = \
                recordspace.join(recordspace.root, f"alias-{alias}-stage-{lifecycle_stage.name}-task_id-{task_id}-datetime-{datestr}.parquet")
            if self.verbose:
                print(f"DBX LIFECYCLE: {lifecycle_stage.name}: Writing build record at lifecycle_stage {lifecycle_stage.name} to {record_filepath}")
            record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)
        return _write_record_lifecycle_callback

    def datablock_range_cls(self):
        #DEBUG
        #pdb.set_trace()
        datablock_cls = self.datablock_cls
        rangecls = RANGE if not hasattr(datablock_cls.SCOPE, 'RANGE') else datablock_cls.SCOPE.RANGE
        SCOPE_fields = dataclasses.fields(datablock_cls.SCOPE)
        has_ranges = any(issubclass(field.type, rangecls) for field in SCOPE_fields)
        if not has_ranges:
            rangecls = None
        return rangecls

    def databuilder_cls(dbx):
        """
            Using 'dbx' instead of 'self' here to avoid confusion of the meaning of 'self' in different scopes: as a DBX instance and a Databuilder subclass instance.
            This Databuilder subclass factory using `datablock_cls` as implementation of the basic `build()`, `read()`, `valid()`, `metric()` methods.
            >. databuilder gets block_to_shard_keys and batch_to_shard_keys according to datablock_scope and datablock_cls.SCOPE RANGE annotations.

            NB: `not hasattr(Databuilder, 'TOPICS')` <=> `Databuilder.topics == [DEFAULT_TOPIC] == [None]` 
        """ 
        def __init__(self, *args, **kwargs):
            Databuilder.__init__(self, *args, **kwargs)
            self._datablock = None

        def __repr__(self):
            _ = f"{repr(dbx)}.databuilder"
            return _

        def datablock(self): # will be wrapped in property()
            if self._datablock is None:
                #DEBUG
                #pdb.set_trace()
                datablock_cls = dbx.datablock_cls
                self._datablock = datablock_cls(self.dataspace.filesystem, **dbx.datablock_kwargs_)
            return self._datablock

        def get_anchorspace(self):
            revision = signature.tag(DBX.Path(self.revision)) if self.revision is not None else ''
            return self.dataspace.subspace(*self.anchorchain).subspace(f"@{revision}",)

        def datablock_shardroots(self, tagshardscope, ensure=False) -> Union[str, Dict[str, str]]:
            def shardroot(topic):
                shardspace = self._shardspace_(topic, **tagshardscope)
                if ensure:
                    shardspace.ensure()
                return shardspace.root
            datablock_cls = dbx.datablock_cls
            if not hasattr(datablock_cls, 'TOPICS'):
                shardroots = shardroot(None)
            else:
                shardroots = {topic: shardroot(topic) for topic in datablock_cls.TOPICS}
            if dbx.debug:
                print(f"DEBUG: DBX.Databuilder: datablock_shardroots: {shardroots}")
            return shardroots

        def datablock_blockroots(self, tagblockscope, ensure=False) -> Union[str, Dict[str, str]]:
            datablock_cls = dbx.datablock_cls
            range_cls = dbx.datablock_range_cls()
            tagshardscopes = self.scope_to_shards(**tagblockscope)
            if range_cls is None:
                assert len(tagshardscopes) <= 1, f"Multiple shards in blockscope for a datablock with no RANGE in SCOPE"
                blockroots = self.datablock_shardroots(tagshardscopes[0], ensure=ensure)
            else:
                if not hasattr(datablock_cls, 'TOPICS'):
                    blockroots = [self.datablock_shardroots(tagshardscope, ensure=ensure)for tagshardscope in tagshardscopes]
                else:
                    blockroots = None
                    for tagshardscope in tagshardscopes:
                        shardroots = self.datablock_shardroots(tagshardscope, ensure=ensure)
                        if blockroots is None:
                            blockroots = {topic: [topicshardroot] for topic, topicshardroot in shardroots.items()}
                        else:
                            for topic, topicshardroot in shardroots.items():
                                blockroots[topic].append(topicshardroot)
            return blockroots

        @OVERRIDEN
        def _build_batch_(self, tagbatchscope, **batchscope):
            datablock_batchscope = dbx.datablock_cls.SCOPE(**batchscope) # need to convert batchscope elements to RANGE wherever necessary
            datablock_batchroots = self.datablock_blockroots(tagbatchscope, ensure=True)
            if self.verbose:
                print(f"DBX: building batch for datablock {type(self.datablock)}: {self.datablock}")
            return self.datablock.build(datablock_batchscope, datablock_batchroots)

        @OVERRIDEN
        def _build_batch_request_(self, tagbatchscope, **batchscope):
            datablock_cls = dbx.datablock_cls
            request = Databuilder._build_batch_request_(self, tagbatchscope, **batchscope)
            if hasattr(datablock_cls, 'summary'):
                request = request.set(summary=datablock_cls.summary)
            return request

        @OVERRIDEN
        def _read_block_(self, tagblockscope, topic, **blockscope):
            datablock_cls = dbx.datablock_cls
            # tagscope can be a list, opaque to the Request evaluation mechanism, but batchscope must be **-expanded to allow Request mechanism to evaluate the kwargs
            datablock_blockroots = self.datablock_blockroots(tagblockscope)
            datablock_blockscope = datablock_cls.SCOPE(**blockscope) # to ensure upconversion to RANGE wherever necessary
            if topic == None:
                assert not hasattr(datablock_cls, 'TOPICS'), f"_read_block_: None topic when datablock.TOPICS == {datablock_cls.TOPICS} "
                _ = self.datablock.read(datablock_blockscope, datablock_blockroots)
            else:
                _ = self.datablock.read(datablock_blockscope, datablock_blockroots, topic)
            return _

        @OVERRIDEN
        def _block_extent_page_(self, topic, **blockscope):
            valid = self._block_value_page_(self.datablock.valid, topic, **blockscope)
            block_intent_page = self.block_intent_page(topic, **blockscope)
            block_extent_page = {kvhandle: shard_pathset 
                for i, (kvhandle, shard_pathset) in enumerate(block_intent_page.items())
                if valid[i]
            }
            return block_extent_page

        @OVERRIDEN
        def _block_metric_page_(self, topic, **blockscope):
            def callable(*args, **kwargs):
                valid = self.datablock.valid(*args, **kwargs)
                if dbx.datablock_range_cls() is None:
                    metric = int(valid)
                else:
                    metric = [int(v) for v in valid]
                return metric
            metric = self._block_value_page_(callable, topic, **blockscope)
            block_intent_page = self.block_intent_page(topic, **blockscope)
            block_metric_page = {kvhandle: metric[i]
                for i, kvhandle in enumerate(block_intent_page.keys())
            }
            return block_metric_page
        
        def _block_value_page_(self, callable, topic, **blockscope):
            """
                #TODO: move to Databuilder.block_extent_page. How to factor?  Through what overridable method?
            """
            if topic != DEFAULT_TOPIC and topic not in self.topics:
                raise UnknownTopic(f"Unknown topic {repr(topic)} is not among {[repr(s) for s in self.topics]}")
            block_intent_page = self.block_intent_page(topic, **blockscope)
            tagblockscope = self.Tagscope(**blockscope)
            datablock_cls = dbx.datablock_cls
            datablock_blockroots = self.datablock_blockroots(tagblockscope)
            datablock_blockscope = datablock_cls.SCOPE(**blockscope)
            if topic == DEFAULT_TOPIC:
                _value = callable(datablock_blockscope, datablock_blockroots)
            else:
                _value = callable(datablock_blockscope, datablock_blockroots, topic)
            #DEBUG
            #pdb.set_trace()
            if dbx.datablock_range_cls() is None:
                value = [_value]
            else:
                value = _value
            return value
        
        datablock_cls = dbx.datablock_cls
        SCOPE_fields = dataclasses.fields(datablock_cls.SCOPE)
        block_keys = [field.name for field in SCOPE_fields]
        block_defaults = {field.name: field.default  for field in SCOPE_fields if field.default != dataclasses.MISSING}
        rangecls = dbx.datablock_range_cls()
        #DEBUG
        #print(f"DEBUG: DBX: databuilder_cls: rangecls: {rangecls}")
        if rangecls is not None:
            block_to_shard_keys = {
                field.name: (field.type.singular if hasattr(field.type, 'singular') else field.name) 
                for field in SCOPE_fields if issubclass(field.type, rangecls)
            }
        else:
            block_to_shard_keys = {}
        if len(block_to_shard_keys) > 0:
            key = next(iter(block_to_shard_keys.keys()))
            batch_to_shard_keys = {key: block_to_shard_keys[key]}
        else:
            batch_to_shard_keys = {}

        __module__ = DBX_PREFIX + "." + dbx._datablock_module_name
        __cls_name = datablock_cls.__name__

        __revision = dbx.revision
        databuilder_classdict = {
                    '__module__': __module__,
                    'block_keys': block_keys, 
                    'block_defaults': block_defaults,
                    'block_to_shard_keys': block_to_shard_keys,
                    'batch_to_shard_keys': batch_to_shard_keys,
                    '__init__': __init__,
                    '__repr__': __repr__,
                    'revision': __revision,
                    'topics': dbx.topics,
                    'datablock': property(datablock),
                    'datablock_blockroots': datablock_blockroots,
                    'datablock_shardroots': datablock_shardroots,
                    '_build_batch_': _build_batch_,
                    '_build_batch_request_': _build_batch_request_,
                    '_read_block_':  _read_block_,
                    '_block_value_page_': _block_value_page_,
        }
        databuilder_classdict['get_anchorspace'] = get_anchorspace
        if hasattr(datablock_cls, 'valid'):
            databuilder_classdict['_block_extent_page_'] = _block_extent_page_
        if hasattr(datablock_cls, 'metric'):
            databuilder_classdict['_block_metric_page_'] = _block_metric_page_
        databuilder_class = type(__cls_name, 
                               (Databuilder,), 
                               databuilder_classdict,)
        return databuilder_class

    def transcribe(self, with_env=(), with_linenos=False, with_display=False, verbose=False, with_build=False):
        return DBX.Transcribe(self, with_env=with_env, with_linenos=with_linenos, with_display=with_display, 
                              with_build=with_build, verbose=verbose,)

    @staticmethod
    def Transcribe(*dbxs, with_env: Tuple[str] = (), with_linenos=False, with_build=False, with_display=tuple(), verbose=False, ):
        """
            Assume dbxs are ordered in the dependency order and all have unique aliases that can be used as variable prefixes.
            TODO: build the dependency graph and reorder, if necessary.
            Examples:
            * Bash definitions (pic dataspace DATABLOCKS_PICLAKE)
            ```
            export MIRCOHN="datablocks.DBX('datablocks.test.micron.datasets.miRCoHN', 'mircohn').Databuilder(dataspace=datablocks.DATABLOCKS_PICLAKE).Datablock(verbose=True)"
            export MIRCOS="datablocks.DBX('datablocks.test.micron.datasets.miRCoStats', 'mircoshn').Databuilder(dataspace=datablocks.DATABLOCKS_PICLAKE).Datablock(verbose=True).SCOPE(mirco=$MIRCOHN.READ('counts'))"
            ```
            * Echo expanded definitions
            ```
            dbx.echo "$MIRCOS"
            ```
            * Print transcript with line numbers:
            ```
            dbx "datablocks.DBX.transcribe($MIRCOS.Databuilder(dataspace=datablocks.DATABLOCKS_PICLAKE).Datablock(verbose=True), with_env=['HOME'], with_linenos=True)" 
            ```
            * Execute transcript (no line numbers allowed)
                . Note the stderr redirect -- it is necessary as some transcript seems to be output via stderr
            ```
            dbx "datablocks.DBX.transcribe($MIRCOS.Databuilder(dataspace=datablocks.HOMELAKE).Datablock(verbose=True), with_env=['HOME'])" 2>&1 | python -
            ```
        """

        script_ = ""

        imports = {}
        env = ""
        read = ""
        build = ""

        if with_env:
            imports['os'] = "import os\n"
        imports['fsspec']= "import fsspec\n"
        
        if with_env:
            for ekey in with_env:
                env += f"{ekey} = os.getenv('{ekey}')\n"

        def transcribe_roots(filesystem, roots, *, prefix=''):
            _roots = ""
            if isinstance(roots, dict):
                _roots += f"{prefix}{{\n"
                for key, val in roots.items():
                    prefix_ = prefix + '\t\t'
                    _roots += f"{prefix}\t{repr(key)}: {transcribe_roots(filesystem, val, prefix=prefix_)},\n"
                _roots += f"\t{prefix}}}"
            elif isinstance(roots, list):
                _roots += "[" + ", ".join(transcribe_roots(filesystem, r) for r in roots) + "]"
            else:
                _roots = "f'"+ roots + "'"
            return _roots       

        for dbx in dbxs:
            imports[dbx._datablock_module_name]= f"import {dbx._datablock_module_name}\n"
            _datablock = f"{dbx.alias}"
            build += f"# {tag(dbx)}\n"
            blockscope = dbx.databuilder._blockscope_(**dbx.scope)
            tagscope = dbx.databuilder._tagscope_(**blockscope)

            if len(dbx.scope):
                _blockscope  = f"{dbx._datablock_clstr}.SCOPE(\n"
                for key, arg in dbx.scope.items():
                    if isinstance(arg, DBX.PathRequest):
                        if arg.topic is None:
                            path = f"{arg.dbx.alias}.roots"
                        else:
                            path = f"{arg.dbx.alias}.roots[{repr(arg.topic)}]"
                        _blockscope += f"\t\t{key}={path},\n"
                    elif isinstance(arg, DBX.ReadRequest):
                        topicarg = f"{repr(arg.topic)}" if arg.topic is not None else ""
                        _blockscope += f"\t\t{key}={arg.dbx.alias}.read({topicarg}),\n"
                    else:
                        _blockscope += f"\t\t{key}={repr(arg)},\n"
                _blockscope += "\t)"
            else:
                _blockscope = ""
            
            filesystem = dbx.databuilder.dataspace.filesystem
            shardroots = dbx.databuilder.datablock_shardroots(tagscope)
            _shardroots = transcribe_roots(filesystem, shardroots, prefix='\t')

            if filesystem.protocol != "file":
                protocol = dbx.databuilder.dataspace.protocol
                storage_options = dbx.databuilder.dataspace.storage_options
                _filesystem = signature.Tagger().tag_func("fsspec.filesystem", [protocol], storage_options) 
            else:
                _filesystem = ""        
            _, __kwargs = Tagger().tag_args_kwargs([], dbx.datablock_kwargs_)
            _kwargs = ""
            for key, val in __kwargs.items():
                _kwargs += f"{key}={val},\n"
            
            build += f"{_datablock} = " + Tagger().ctor_name(dbx.datablock_cls) + "(\n"     + \
                            (f"\troots={_shardroots},\n" if len(_shardroots) > 0 else "")      + \
                            (f"\tfilesystem={_filesystem},\n" if len(_filesystem) > 0 else "") + \
                            (f"\tscope={_blockscope},\n" if len(_blockscope) > 0 else "")      + \
                            (f"\t{_kwargs}")                                                + \
                     "\n)\n"
            build += "\n"

            script = ''.join(imports.values()) + "\n\n"
            if len(with_env):
                script += env + "\n\n"
            #script += read + "\n"
            script += build

            if with_linenos: 
                lines = script.split('\n')
                #width = round(int(math.log(len(lines))))
                script_ = '\n'.join([f"{i:>4}: " + ln for i, ln in enumerate(lines)]) #TODO: use width for padding
            else:
                script_ = script

            if with_build:
                script_ += f"{_datablock}.build()"

        return script_
    

PICLAKE = DATALAKE.clone("{DATALAKE}")
def TRANSCRIBE(dbx, piclake=PICLAKE):
    print(dbx.clone(dataspace=piclake, pic=True, verbose=False).Datablock(verbose=True).transcribe(with_build=True))
            
            

            
    
    