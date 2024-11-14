from collections import Iterable
import copy
import dataclasses
from dataclasses import dataclass, asdict, replace, field
import functools
import hashlib
import importlib
import logging
import os
import pdb #DEBUG
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
from .utils import OVERRIDE, serializable, microseconds_since_epoch, datetime_to_microsecond_str
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

from .utils import gitrepostate


class DEFAULT_ARG:
    pass

DEFAULT = DEFAULT_ARG


class DBXEvalError(Exception):
    pass

def dbx_eval(string):
    """
        A version of __builtins__.eval() in the context of impoirted datablocks.datablock.
    """
    import datablocks.dbx
    _eval = __builtins__['eval']
    try:
        _ = _eval(string)
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
    REVISION = '0.0.1'
    PATHNAME = "data.ext" # define this or define TOPICS: dict

    @dataclass
    class SCOPE:
        ...

    def __init__(self, 
                 roots=None, 
                 filesystem:fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
                 scope=None,
                 *, 
                 verbose=False, 
                 debug=False, 
                 rm_tmp=True, ):
        self.scope = scope
        self.roots = roots
        self.filesystem = filesystem
        self.verbose = verbose
        self.debug = debug
        self.rm_tmp = rm_tmp

    def build(self):
        raise NotImplementedError

    def read(self, topic=None):
        raise NotImplementedError

    def valid(self, topic=None):
        if hasattr(self, 'TOPICS'):
            path = self.path(topic)
            _ = self.filesystem.exists(path)
        else:
            path = self.path()
            _ = self.filesystem.exists(path)
        return _            
    
    '''
    #TODO: fix dbx.__extent_shard_metric__, which fails with this
    def metric(self, topic=None):
        return int(self.valid(topic))
    '''

    def built(self):
        built = True
        if hasattr(self, 'TOPICS'):
            for topic in self.TOPICS:
                if not self.valid(topic):
                    self.print_verbose(f"Topic {topic} not built")
                    built = False
                    break
        else:
            if not self.valid():
                built = False
        return built

    def path(self, topic=None):
        roots = self.roots
        filesystem = self.filesystem
        if topic is not None:
            if filesystem.protocol == 'file':
                if roots is None:
                    path = os.path.join(os.getcwd(), self.TOPICS[topic])
                else:
                    path_ = roots[topic]
                    os.makedirs(path_, exist_ok=True)
                    path = os.path.join(path_, self.TOPICS[topic])
            else:
                path = roots[topic] + "/" + self.TOPICS[topic]
        else:
            if filesystem.protocol == 'file':
                if roots is None:
                    path = os.join(os.getcwd(), self.PATHNAME)
                else:
                    path_ = roots
                    os.makedirs(path_, exist_ok=True)
                    path = os.path.join(path_, self.PATHNAME)
            else:
                path = roots + "/" + self.PATHNAME
        return path

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

    class ProxyRequest(request.Proxy):
        @staticmethod
        def parse_url(url: str, **datablock_kwargs):
            #url = {classtr}@{revision}[#alias]:{topic}
            tail = url
            try:
                datablock_clstr, tail  = tail.split("@")
            except:
                raise ValueError(f"Malformed url: {url}")
            try:
                revision_alias, topic   = tail.split(":")
                if revision_alias.find('#') == -1:
                    revision = revision_alias
                    alias = None
                else:
                    revision, alias = revision_alias.split('#')
                if revision == 'None':
                    revision = None
            except:
                raise ValueError(f"Malformed url: {url}")
            if len(alias) == 0:
                alias = None
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
            tag___ = f"{DBX_PREFIX}.{self.dbx._datablock_clstr}"
            tag__ = tag___+f"@{self.dbx.databuilder.revision}"
            tag_ = tag__ + f"#{self.dbx.databuilder.alias if self.dbx.databuilder.alias else ''}"
            tag = tag_ + f":{self.topic}" if self.topic != DEFAULT_TOPIC else tag_+""
            return tag
        
    class ReadRequest(ProxyRequest):
        def __init__(str, spec: Union[str, tuple], pic=False):
            super().__init__('READ', spec, pic=pic)

    class PathRequest(ProxyRequest):
        def __init__(str, spec: Union[str, tuple], pic=False):
            super().__init__('PATH', spec, pic=pic)

    @staticmethod
    def show_datablocks(*, dataspace=DATALAKE, pretty_print=True, debug=False):
        def _chase_anchors(_dataspace, _anchorchain=()):
            pathnames = _dataspace.list()
            anchorchains = []
            for pathname in pathnames:
                if pathname.startswith('.'):
                    continue
                elif _dataspace.isfile(pathname, relative=True):
                    continue
                elif pathname.startswith('revision='):
                    anchorchain = _anchorchain + (pathname,)
                    anchorchains.append(anchorchain)
                    if debug:
                        print(f"DEBUG: show_datablocks(): chase_anchors(): addded anchorchain {anchorchain} with terminal pathname {pathname}")
                else:
                    dataspace_ = _dataspace.subspace(pathname)
                    anchorchain_ = _anchorchain+(pathname,)
                    _anchorchains = _chase_anchors(dataspace_, anchorchain_)
                    anchorchains.extend(_anchorchains)
            return anchorchains
        
        datablock_dataspace = dataspace.subspace(DBX_PREFIX).ensure()

        anchorchains = _chase_anchors(datablock_dataspace)
        if debug:
            print(f"DEBUG: show_datablocks(): anchorchains: {anchorchains}")

        datablocks = {}
        for anchorchain in anchorchains:
            path = '.'.join(anchorchain[:-1])
            revision = anchorchain[-1]
            if path in datablocks:
                datablocks[path].append(revision)
            else:
                datablocks[path] = [revision]
        if pretty_print:
                for key, value in datablocks.items():
                    print(f"{key}: {value}")
        else:
            return datablocks
    @dataclass
    class State:
        spec: Union[Datablock, str]
        alias: Optional[str] = None
        pic: bool = False
        use_alias_dataspace:bool = False
        repo: Optional[str] = None
        revision: Optional[str] = None
        verbose: Optional[str] = False
        debug: bool = False
        scope_: Optional = None
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

    def __init__(self, 
                spec: Optional[Union[type, str]] = None, #Datablock cls or clstr
                alias=None,
                *,
                pic=False,
                use_alias_dataspace=True,
                repo=None,
                revision=None,
                verbose=False,
                debug=False,  
    ):
        self.__setstate__(DBX.State(spec=spec,
                                alias=alias,
                                pic=pic,
                                use_alias_dataspace=use_alias_dataspace,
                                repo=repo,
                                revision=revision,
                                verbose=verbose,
                                debug=debug
        ))
        
    def __getstate__(self):
        return DBX.State(spec=self.spec,
                         alias=self.alias,
                         pic=self.pic,
                         use_alias_dataspace=self.use_alias_dataspace,
                         repo=self.repo,
                         revision=self.revision,
                         verbose=self.verbose,
                         debug=self.debug,
                         scope_=self.scope_,
                         databuilder_kwargs_=self.databuilder_kwargs_,
                         datablock_kwargs_=self.datablock_kwargs_,
                         datablock_scope_kwargs_=self.datablock_scope_kwargs_,
        )

    def __setstate__(self, 
                     state: State, 
    ):
        for key, val in asdict(state).items():
            setattr(self, key, val)

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
            self.datablock_scope_kwargs_.update(**kwargs)
            return self
        self.SCOPE = update_scope

        if self.databuilder_kwargs_ is None:
            self.databuilder_kwargs_ = {}
        @functools.wraps(Databuilder)
        def update_databuilder_kwargs(**kwargs):
            self.databuilder_kwargs_.update(**kwargs)
            return self
        self.Databuilder = update_databuilder_kwargs

        self._datablock_module_name = None
        self._datablock_clstr = None
        self._datablock_clsname = None
        self._datablock_cls = None

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
              use_alias_dataspace=DEFAULT,
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
        if use_alias_dataspace != DEFAULT:
            kwargs['use_alias_dataspace'] = use_alias_dataspace
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
        #TODO: add pic?
        _tag = f"{DBX_PREFIX}.{self.spec}"
        if self.alias is not None:
            _tag += f"#{self.alias}"
        tag = f"'{_tag}'"
        return tag

    def __hash__(self):
        #TODO think this through
        prefixstr = f"{self.__class__}--{self.spec}--{self.alias}"
        statestr = pickle.dumps(self.__getstate__())
        hashstr = prefixstr + statestr
        _ =  int(hashlib.sha1(hashstr.encode()).hexdigest(), 16)
        return _

    def __getattr__(self, attr):
        try:
            #DEBUG
            #pdb.set_trace()
            datablock_clsname = super().__getattribute__('_datablock_clsname')
            if attr == datablock_clsname:
                return self.Datablock
            else:
                super().__getattribute__(attr)
        except:
            return super().__getattribute__(attr)

    def datablock_cls(self):
        #pdb.set_trace() #DEBUG
        if self._datablock_cls is None:
            with gitrepostate(self.repo, self.revision, verbose=self.verbose):
                mod = importlib.import_module(self._datablock_module_name)
            self._datablock_cls = getattr(mod, self._datablock_clsname)
        return self._datablock_cls

    @property
    def databuilder(self):
        databuilder_cls = self.databuilder_cls()
        databuilder_kwargs = copy.copy(self.databuilder_kwargs_)
        if 'dataspace' in databuilder_kwargs:
            databuilder_kwargs['dataspace'] = databuilder_kwargs['dataspace'].with_pic(True) #TODO: why with_pick(True)?
        databuilder = databuilder_cls(self.alias, 
                                      build_block_request_lifecycle_callback=self._build_block_request_lifecycle_callback_,
                                      **databuilder_kwargs)
        return databuilder

    @property
    def scope(self):
        if self.scope_ is not None:
            _scope = self.scope_
            if self.verbose:
                print(f"DBX: scope for datablock with alias {repr(self.databuilder.alias)}: using specified scope: {self.scope_}")
        else:
            record = self.show_named_record(alias=self.databuilder.alias, revision=self.revision, stage='END') 
            if record is not None:
                try:
                    _scope = dbx_eval(record['scope'])
                except DBXEvalError as ee:
                    raise ValueError(f"Failed to parse build scope {record['scope']}") from ee
                if self.verbose:
                    print(f"DBX: scope: no specified scope for {self} with datablock with alias {repr(self.databuilder.alias)}")
                    print(f"DBX: scope: using build record scope: {_scope}")
            else:
                _scope = asdict(self.datablock_cls().SCOPE(**self.datablock_scope_kwargs_))
                if self.verbose:
                    print(f"DBX: scope: no specified scope and no build records for {self} with alias {repr(self.databuilder.alias)}")
                    print(f"DBX: scope: constructing scope from kwargs:\n{self.datablock_scope_kwargs_}")
            self.scope_ = _scope
        return _scope
    
    @property
    def tagscope(self):
        _ = self.databuilder._tagscope_(**self.scope)
        return _

    @property
    def topics(self):
        if hasattr(self.datablock_cls(), 'TOPICS'):
            if isinstance(self.datablock_cls().TOPICS, dict):
                topics = list(self.datablock_cls().TOPICS.keys())
            else:
                topics = self.datablock_cls().TOPICS
        else:
            topics = [DEFAULT_TOPIC]
        return topics

    def build(self):
        try:
            request = self.build_request()
            response = request.evaluate()
        except UnknownTopic as e:
            raise ValueError(f"Unknown topic error: wrong/stale scope?") from e

        if self.verbose:
            print(f"response_id: {response.id}")
        response.result()

    def build_request(self):
        import datablocks #TODO: why the local import?
        def scopes_equal(s1, s2):
            #DEBUG
            #pdb.set_trace()
            if set(s1.keys()) != (s2.keys()):
                return False
            for key in s1.keys():
                if s1[key] != s2[key]:
                    return False
            return True
        # TODO: consistency check: sha256 alias must be unique for a given revision or match scope
        blockscope = self.databuilder._blockscope_(**self.scope)
        record = self.show_named_record(alias=self.databuilder.alias, revision=self.revision, stage='END') 
        if record is not None:
            try:
                _scope = dbx_eval(record['scope'])
            except Exception as e:
                #TODO: examples of what causes these exceptions
                if self.verbose:
                    print(f"Failed to retrieve scope from build record, ignoring scope of record.")
                _scope = None
            if _scope is not None and not scopes_equal(blockscope, _scope):
                raise ValueError(f"Attempt to overwrite prior scope {_scope} with {blockscope} for {self.datablock_cls()} alias {self.alias}")
        request = self.databuilder.build_block_request(**blockscope)
        return request
    
    def path(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"{self} has not been built yet")
        tagscope = self.databuilder._tagscope_(**self.scope)
        page = self.databuilder.block_extent_page(topic, **tagscope)
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
    
    @property
    def shortfall(self):
        _ = self.databuilder.block_shortfall(**self.scope)
        return _
    
    @property
    def metric(self):
        _ = self.databuilder.block_extent_metric(**self.scope)
        return _

    @property
    def valid(self):
        _ = self.extent
        return _

    BUILD_RECORDS_COLUMNS_SHORT = ['stage', 'revision', 'scope', 'alias', 'task_id', 'metric', 'status', 'date', 'timestamp', 'runtime_secs']

    def show_build_records(self, *, stage='ALL', full=False, columns=None, all=False, tail=5):
        """
        All build records for a given Databuilder class, irrespective of revision (see `show_build_record()` for retrieval of more specific information).
        'all=True' forces 'tail=None'
        """
        short = not full
        recordspace = self._recordspace_()

        frame = None
        try:
            
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

    def show_build_record_columns(self, *, full=True, **ignored):
        frame = self.show_build_records(full=full)
        columns = frame.columns
        return columns

    def show_build_record(self, record=None, *, full=False, stage='ALL'):
        import datablocks
        try:
            records = self.show_build_records(full=full, stage=stage)
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

    def show_named_record(self, *, alias=None, revision=None, full=False, stage=None):
        import datablocks # needed for `eval`
        records = self.show_build_records(full=full, stage=stage)
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
    
    def show_build_graph(self, record=None, *, node=tuple(), show_=('logpath', 'logpath_status', 'exception'), show=tuple(), **kwargs):
        if not isinstance(node, Iterable):
            node = (node,)
        _record = self.show_build_record(record=record, full=True)
        if _record is None:
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
    
    def show_build_transcript(self, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        if _record is None:
            summary = None
        else:
            summary = _record['report_transcript']
        return summary

    def show_build_scope(self, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        if _record is not None:
            scopestr = _record['scope']
            try:
                scope = dbx_eval(scopestr)
            except DBXEvalError as ee:
                raise ValueError(f"Failed to parse build scope {scopestr}") from ee
        else:
            scope = None
        return scope
    
    def UNSAFE_clear_build_records(self):
        self._recordspace_().remove()
    
    def UNSAFE_clear_request(self):
        blockscope = self.databuilder._blockscope_(**self.scope)
        tagblockscope = self.databuilder._tagscope_(**blockscope)
        request = Request(self._UNSAFE_clear_block_, **tagblockscope)
        return request

    def UNSAFE_clear(self):
        request = self.UNSAFE_clear_request()
        self.UNSAFE_clear_build_records()
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
                           stage=lifecycle_stage.name,
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
                report = response.report()
                task_id = response.id
                report_transcript = report.transcript() 
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

        def datablock(self, roots, filesystem, scope=None):
            #pdb.set_trace() #DEBUG
            datablock_cls = dbx.datablock_cls()
            self._datablock = datablock_cls(roots, filesystem, scope, **dbx.datablock_kwargs_)
            return self._datablock

        @property
        def revisionspace(self):
            if self.alias is not None and dbx.use_alias_dataspace:
                _ = self.anchorspace.subspace(f"@{str(self.revision)}",f"#{self.alias}", )
            else:
                _ = self.anchorspace.subspace(f"@{str(self.revision)}",)
            return _

        def _alias_shardspace_(self, topic, **shard):
            # ignore shard, label by alias, revision, topic only
            if topic is None:
                _ = self.revisionspace
            else:
                _ = self.revisionspace.subspace(topic)
            if self.debug:
                print(f"ALIAS SHARDSPACE: formed for topic {repr(topic)}: {_}")
            return _

        def datablock_shardroots(self, tagscope, ensure=False) -> Union[str, Dict[str, str]]:
            shardscope_list = self.scope_to_shards(**tagscope)
            assert len(shardscope_list) == 1
            shardscope = shardscope_list[0]
            if hasattr(dbx.datablock_cls(), 'TOPICS'):
                shardroots = {}
                for _topic in dbx.datablock_cls().TOPICS:
                    shardspace = self._shardspace_(_topic, **shardscope)
                    if ensure:
                        shardspace.ensure()
                    shardroots[_topic] = shardspace.root
            else:
                shardspace = self._shardspace_(None, **shardscope)
                if ensure:
                    shardspace.ensure()
                shardroots = shardspace.root
            return shardroots

        def datablock_blockroots(self, tagscope, ensure=False) -> Union[Union[str, List[str]], Dict[str, Union[str, List[str]]]]:
            #TODO: implement blocking: return a dict from topic to str|List[str] according to whether this is a shard or a block
            if len(self.block_to_shard_keys) > 0:
                raise NotImplementedError(f"Batching not supported at the moment: topic: tagscope: {tagscope}")
            blockroots = self.datablock_shardroots(tagscope, ensure=ensure)
            return blockroots

        def datablock_batchroots(self, tagscope, ensure=False) -> Union[Union[str, List[str]], Dict[str, Union[str, List[str]]]]:
            #TODO: implement batching: return a dict from topic to str|List[str] according to whether this is a shard or a batch
            if len(self.batch_to_shard_keys) > 0:
                raise NotImplementedError(f"Batching not supported at the moment: topic: tagscope: {tagscope}")
            batchroots = self.datablock_shardroots(tagscope, ensure=ensure)
            return batchroots

        def _build_batch_(self, tagscope, **batchscope):
            #DEBUG
            #pdb.set_trace()
            datablock_batchscope = dbx.datablock_cls().SCOPE(**batchscope)
            datablock_shardroots = self.datablock_batchroots(tagscope, ensure=True)
            dbk = self.datablock(datablock_shardroots, 
                                 scope=datablock_batchscope, 
                                 filesystem=self.dataspace.filesystem)
            if self.verbose:
                print(f"DBX: building batch for datablock {type(dbk)}: {dbk} constructed with kwargs {dbx.datablock_kwargs_}")
            dbk.build()

        def _read_block_(self, tagscope, topic, **blockscope):
            # tagscope can be a list, opaque to the Request evaluation mechanism, but batchscope must be **-expanded to allow Request mechanism to evaluate the kwargs
            datablock_blockroots = self.datablock_blockroots(tagscope)
            datablock_blockscope = dbx.datablock_cls().SCOPE(**blockscope)
            if topic == None:
                assert not hasattr(dbx.datablock_cls(), 'TOPICS'), f"_read_block_: None topic when datablock.TOPICS == {dbx.datablock_cls().TOPICS} "
                _ = self.datablock(datablock_blockroots, scope=datablock_blockscope, filesystem=self.dataspace.filesystem).read()
            else:
                _ = self.datablock(datablock_blockroots, scope=datablock_blockscope, filesystem=self.dataspace.filesystem).read(topic)
            return _

        def _shard_extent_page_valid_(self, topic, tagscope, **shardscope):
            datablock_tagshardscope = dbx.datablock_cls().SCOPE(**shardscope)
            datablock_shardroots = self.datablock_shardroots(tagscope)
            if topic == None:
                assert not hasattr(dbx.datablock_cls(), 'TOPICS'), \
                    f"__shard_extent_page_valid__: None topic when datablock_cls.TOPICS == {getattr(dbx.datablock_cls(), 'TOPICS')} "
                
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem,).valid()
            else:
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem).valid(topic)
            return _
        
        def _extent_shard_metric_(self, topic, tagscope, **shardscope):
            datablock_tagshardscope = dbx.datablock_cls().SCOPE(**shardscope)
            datablock_shardroots = self.datablock_shardroots(tagscope)
            if topic == None:
                assert not hasattr(self.datablock_cls(), 'TOPICS'), \
                        f"__extent_shard_metric__: None topic when datablock_cls.TOPICS == {getattr(dbx.datablock_cls(), 'TOPICS')} "
                
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem).metric()
            else:
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem).metric(topic)
            return _
   
        rangecls = RANGE if not hasattr(dbx.datablock_cls(), 'RANGE') else dbx.datablock_cls().RANGE
        SCOPE_fields = dataclasses.fields(dbx.datablock_cls().SCOPE)
        block_keys = [field.name for field in SCOPE_fields]
        block_defaults = {field.name: field.default  for field in SCOPE_fields if field.default != dataclasses.MISSING}
        batch_to_shard_keys = {field.name: field.name for field in SCOPE_fields if isinstance(field.type, rangecls)}

        __module__ = DBX_PREFIX + "." + dbx._datablock_module_name
        __cls_name = dbx.datablock_cls().__name__
        #datablock_cls = dbx.datablock_cls 
        #datablock_kwargs = dbx.datablock_kwargs_
        if hasattr(dbx.datablock_cls(), 'TOPICS'):
            topics = dbx.datablock_cls().TOPICS
        else:
            topics = [DEFAULT_TOPIC]

        __revision = dbx.revision
        databuilder_classdict = {
                    '__module__': __module__,
                    'block_keys': block_keys, 
                    'block_defaults': block_defaults,
                    'batch_to_shard_keys': batch_to_shard_keys,
                    '__init__': __init__,
                    '__repr__': __repr__,
                    'revision': __revision,
                    'topics': topics,
                    #'datablock_cls': datablock_cls,
                    #'datablock_kwargs': datablock_kwargs,
                    'datablock': datablock,
                    'datablock_blockroots': datablock_blockroots,
                    'datablock_batchroots': datablock_batchroots,
                    'datablock_shardroots': datablock_shardroots,
                    '_build_batch_': _build_batch_,
                    '_read_block_':  _read_block_,
        }
        databuilder_classdict['revisionspace'] = revisionspace
        #TODO: enable
        #if dbx.use_alias_dataspace:
        #    databuilder_classdict['_shardspace_'] = _alias_shardspace_
        if hasattr(dbx.datablock_cls(), 'valid'):
            databuilder_classdict['_shard_extent_page_valid_'] = _shard_extent_page_valid_
        if hasattr(dbx.datablock_cls(), 'metric'):
            databuilder_classdict['_extent_shard_metric_'] = _extent_shard_metric_

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
            blockroots = dbx.databuilder.datablock_blockroots(tagscope)
            _blockroots = transcribe_roots(filesystem, blockroots, prefix='\t')

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
            
            build += f"{_datablock} = " + Tagger().ctor_name(dbx.datablock_cls()) + "(\n"     + \
                            (f"\troots={_blockroots},\n" if len(_blockroots) > 0 else "")      + \
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
    print(dbx.clone(dataspace=piclake, use_alias_dataspace=True, pic=True, verbose=False).Datablock(verbose=True).transcribe(with_build=True))
            
            

            
    
    