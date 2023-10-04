import collections
from collections.abc import Callable, Iterable
import copy
import dataclasses
from dataclasses import dataclass
import functools
import hashlib
import importlib
import logging
import os

from typing import Any, TypeVar, Generic, Tuple

import fsspec


import pyarrow.parquet as pq
import pandas as pd

from . import signature as tag
from .signature import Signature, ctor_name
from .signature import Tagger
from .utils import DEPRECATED, OVERRIDE, microseconds_since_epoch, datetime_to_microsecond_str
from .eval import request
from .eval.request import Request, Report, LAST, Graph
from .eval.pool import DATABLOCKS_LOGGING_POOL, DATABLOCKS_LOGGING_REDIRECT_POOL
from .dataspace import Dataspace, DATABLOCKS_DATALAKE


_eval = __builtins__['eval']
_print = __builtins__['print']


logger = logging.getLogger(__name__)


HOME = os.environ['HOME']

T = TypeVar('T')
class RANGE(Generic[T], tuple):
    def __init__(self, *args):
        tuple.__init__(args)
        
    
DBX_PREFIX = 'DBX'


class Datablock:
    VERSION = '0.0.0'
    DEFAULT_ROOT = os.getcwd()
    DEFAULT_FILESYSTEM = fsspec.filesystem("file")
    DEFAULT_TOPIC = None

    @dataclass
    class SCOPE:
        """
            - In DBX.build() scopes containing RANGE members beyond those annotated as RANGE in SCOPE are treating as `blocking` arguments,
              to be broken up along to form batches for building.  This means DBX creates one Datablock instance per batch and calls .build() on it.
            - In DBX.read() scopes containing RANGE members are used as is to create a single Datablock instance to .read().  The instance is free
              to reject RANGE-containing scope, if it cannot collate individual shards.
        """
        ...
        # Example:
        #instruments: RANGE[str] = RANGE(('x', 'y'))


    def build(self, blockscope: SCOPE, filesystem: fsspec.AbstractFileSystem, *roots: Tuple[str]):
        ...
    
    def read(self, topic, blockscope: SCOPE, filesystem: fsspec.AbstractFileSystem, *roots: Tuple[str]):
        ...

    def valid(self, topic, shardscope: SCOPE, filesystem: fsspec.AbstractFileSystem, root: str):
        ...

    def metric(self, topic, shardscope: SCOPE, filesystem: fsspec.AbstractFileSystem, root: str) -> float:
        ...


class Anchored:
    def __init__(self, namechain=None):
        self.namechain = namechain

    @property
    def anchorchain(self):
        if not hasattr(self, 'anchor'):
            #print(f"DEBUG: self.anchor: self.__class__.__module__: {self.__class__.__module__}, self.__class__.__qualname__: {self.__class__.__qualname__}")
            modchain = tuple(str(self.__class__.__module__).split('.'))
            anchorclassname = self.__class__.__qualname__.split('.')[-1]
            anchorclasschain = modchain + (anchorclassname,)
        else:
            anchorclasschain = self.anchor
        if self.namechain:
            anchorchain = anchorclasschain + self.namechain
        else:
            anchorchain = anchorclasschain
        #print(f"DEBUG: anchorchain: {anchorchain}")
        return anchorchain


class Scoped:
    # TODO: implement support for multiple batch_to_shard_keys
    block_keys = []
    block_to_shard_keys = {}
    batch_to_shard_keys = {}
    block_defaults = {} 
    block_pins = {}  

    def __init__(self):
        if hasattr(self, 'block_to_shard_keys'):
            self.shard_to_block_keys = {val: key for key, val in self.block_to_shard_keys.items()}
        elif hasattr(self, 'shard_to_block_keys'):
            self.block_to_shard_keys = {val: key for key, val in self.shard_to_block_keys.items()}
        else:
            self.block_to_shard_keys = {}
            self.shard_to_block_keys = {}
        if hasattr(self, 'block_keys'):
            self.shard_keys = [key if key not in self.block_to_shard_keys else self.block_to_shard_keys[key]
                               for key in self.block_keys]
        elif hasattr(self, 'shard_keys'):
            self.block_keys = [key if key not in self.shard_to_block_keys else self.shard_to_block_keys[key]
                               for key in self.shard_keys]
        else:
            raise ValueError("`block_keys` or `shard_keys` must be specified")

    @property
    def batch_by_block_key(self):
        if len(self.batch_to_shard_keys) > 1:
            raise NotImplementedError(f"Batching by multiple keys is not supported")
        if len(self.batch_to_shard_keys) == 1:
            batch_by_block_key = list(self.batch_to_shard_keys.keys())[0]
        else:
            batch_by_block_key = None
        return batch_by_block_key

    # TODO: collect scope normalization code from _block2* to here
    def _blockscope_(self, **scope):
        def blockify(key, val):
            if key in self.shard_to_block_keys:
                _key = self.shard_to_block_keys[key]
                _val = RANGE([val]) 
            elif key in self.block_keys:
                _key, _val = key, val
            else:
                raise ValueError(f"key={key} with val={val} is neither a block key nor a shard key")
            return _key, _val
        _blockscope = {blockify(key, val)[0]: blockify(key, val)[1] for key, val in scope.items()}
        block_ = {}
        for key in self.block_keys:
            if key in _blockscope:
                key_ = key
                val_ = _blockscope[key]
            elif key in self.block_defaults:
                key_ = key
                val_ = self.block_defaults[key]
            elif key in self.block_pins:
                key_ = key
                val_ = self.block_pins[key]
            else:
                raise ValueError(f"block_key={key} is neither in blockified _blockscope={_blockscope} no in block_defaults={self.block_defaults}")
            if key_ in self.block_pins and val_ != self.block_pins[key_]:
                raise ValueError(f"block key {key_} has value {val_}, which contradicts pinned value {self.block_pins[key_]}")
            block_[key_] = val_
        return block_

    def _tagscope_(self, **scope):
        tagscope = Tagger().tag_dict(scope)
        return tagscope

    def scope_to_shards(self, **scope):
        def _scope2kwargs(scope, plural_key_counters):
            kwargs = {}
            for key, val in scope.items():
                if key in self.block_to_shard_keys:
                    skey = self.block_to_shard_keys[key]
                    counter = plural_key_counters[key]
                    kwargs[skey] = val[counter]
                else:
                    skey = key
                    kwargs[skey] = val
            return kwargs

        for key in scope.keys():
            if key not in self.shard_keys and key not in self.block_to_shard_keys:
                raise ValueError(f"Unknown key {key} is not in databook keys {list(self.shard_keys)}"
                                 f"or among the plurals of any known key {list(self.block_to_shard_keys.keys())}")
            if key in self.block_to_shard_keys and not isinstance(scope[key], collections.Iterable):
                raise ValueError(f"Value for plural key {key} is not iterable: {scope[key]}")

        # Normalize keyvals to plural, wherever possible
        scope_ = {}
        for key, val in scope.items():
            if key in self.shard_to_block_keys:
                key_, val_ = self.shard_to_block_keys[key], [val]
            else:
                key_, val_ = key, val
            scope_[key_] = val_

        # Set scope keys in the order of self.keys
        _scope = {key: scope_[key] for key in self.scope_keys}

        plural_key_counters = {key: 0 for key in reversed(self.scope_keys)
                               if key in self.block_to_shard_keys}
        kwargs_list = []
        carry = False
        while not carry:
            kwargs = _scope2kwargs(_scope, plural_key_counters)
            kwargs_list.append(kwargs)
            if len(plural_key_counters) > 0:
                for key in plural_key_counters:
                    plural_key_counters[key] += 1
                    if plural_key_counters[key] < len(_scope[key]):
                        carry = False
                        break
                    else:
                        plural_key_counters[key] = 0
                        carry = True
            else:
                carry = True
        return kwargs_list

    # TODO: --> _handles2batches_?
    def _kvchains2batches_(self, *kvchains):
        # We assume that all keys are always the same -- equal to self.keys
        # Nontrivial grouping of kwargs into batches is possible only when cls.batch_by_plural_key is in cls.keys
        if len(kvchains) == 0:
            batch_list = []
        else:
            vals_list = [tuple(val for _, val in kvhandle) for kvhandle in kvchains]
            batch_list = []
            # TODO: all these checks in __init__
            if self.batch_by_block_key is not None and \
                    self.batch_by_block_key in self.block_to_shard_keys and \
                    self.batch_by_block_key in self.scope_keys:
                batch_by_singular_key = self.block_to_shard_keys[self.batch_by_block_key]
                groupbykeys = [skey for skey in self.shard_keys if skey != batch_by_singular_key]
                if len(groupbykeys) > 0:
                    frame = pd.DataFrame.from_records(vals_list, columns=self.shard_keys)
                    groups = frame.groupby(groupbykeys)
                    for groupbyvals, groupframe in groups:
                        if len(groupbykeys) == 1:
                            groupbyvals = (groupbyvals,)
                        batch = {groupbykeys[i]: groupbyvals[i] for i in range(len(groupbykeys))}
                        batch[self.batch_by_block_key] = groupframe[batch_by_singular_key].tolist()
                        batch_list.append(batch)
                else:
                    batch_list = [{self.batch_by_block_key: [vals[0] for vals in vals_list]}]
            else:
                for vals in vals_list:
                    batch = {self.shard_keys[i]: vals[i] for i in range(len(self.shard_keys))}
                    batch_list.append(batch)
        return batch_list


class KeyValHandle(tuple):
    def __call__(self, *args, **kwargs) -> Any:
        return super().__call__(*args, **kwargs)
    
    def __str__(self):
        dict_ = {key: val for key, val in self}
        str_ = f"{dict_}"
        return str_


DEFAULT_TOPIC = None
DEFAULT_VERSION = '0.0.0'
RECORD_SCHEMA_VERSION = '0.0.0'
class Databook(Anchored, Scoped): 
    # TODO: implement support for multiple batch_to_shard_keys
    record_schema = RECORD_SCHEMA_VERSION
    topics = []
    signature = Signature((), ('dataspace', 'version',)) # extract these attrs and use in __tag__

    # TODO: make dataspace, version position-only and adjust Signature
    def __init__(self,
                 alias=None,
                 *,
                 dataspace=DATABLOCKS_DATALAKE,
                 tmpspace=None,
                 version=DEFAULT_VERSION,
                 topics=None,
                 lock_pages=False,
                 throw=True,
                 rebuild=False,
                 verbose=False,
                 pool=DATABLOCKS_LOGGING_REDIRECT_POOL):
        Anchored.__init__(self)
        Scoped.__init__(self)
        self.version = version
        self.alias = alias
        self.dataspace = dataspace
        self._tmpspace = tmpspace
        self.anchorspace = dataspace.subspace(*self.anchorchain)
        self.versionspace = self.anchorspace.subspace(f"version={str(self.version)}",)
        #self.logspace = self.versionspace.subspace(*pool.anchorchain)
        self.lock_pages = lock_pages
        self.verbose = verbose
        if topics is None:
            self.topics = [DEFAULT_TOPIC]
        self.pool = pool
        self.reload = rebuild
        self.throw = throw
        if self.lock_pages:
            raise NotImplementedError("self.lock_pages not implemented for Databook")
        if hasattr(self, 'block_to_shard_keys'):
            self.shard_to_block_keys = {val: key for key, val in self.block_to_shard_keys.items()}
        elif hasattr(self, 'shard_to_block_keys'):
            self.block_to_shard_keys = {val: key for key, val in self.shard_to_block_keys.items()}
        else:
            self.block_to_shard_keys = {}
            self.shard_to_block_keys = {}
        if hasattr(self, 'scope_keys'):
            self.shard_keys = [key if key not in self.block_to_shard_keys else self.block_to_shard_keys[key]
                               for key in self.scope_keys]
        elif hasattr(self, 'shard_keys'):
            self.scope_keys = [key if key not in self.shard_to_block_keys else self.shard_to_block_keys[key]
                               for key in self.shard_keys]
        else:
            raise ValueError("`scope_keys` or `shard_keys` must be specified")

    def __repr__(self):
        return self.__tag__()

    def __tag__(self):
        if hasattr(self, 'signature'):
            schema = self.signature.schema
            argattrs = schema[0]
            args = [getattr(self, attr) for attr in argattrs]
            kwargattrs = schema[1]
            kwargs = {attr: getattr(self, attr) for attr in kwargattrs}
            repr = tag.Tagger().tag_ctor(self.__class__, *args, **kwargs)
            return repr
        else:
            repr = tag.Tagger().tag_ctor(self.__class__)

    @property
    def tmpspace(self):
        if self._tmpspace is None:
            self._tmpspace = self.versionspace.temporary(self.versionspace.subspace('tmp').ensure().root)
        return self._tmpspace

    #TODO: #MOVE -> DBX.show_topics()
    def get_topics(self, print=False):
        if print:
            __build_class__['print'](self.topics)
        return self.topics

    #TODO: #MOVE -> DBX.show_version()
    def get_version(self, print=False):
        if print:
            __build_class__['print'](self.topics)
        return self.version

    #RENAME: -> block_page_intent
    def intent_datapage(self, topic, **scope):
        blockscope = self._blockscope_(**scope)
        tagblockscope = self._tagscope_(**blockscope)
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        shard_list = self.scope_to_shards(**tagblockscope)
        kvhandle_pathshard_list = []
        for shard in shard_list:
            kvhandle = self._scope_to_kvchain_(topic, **shard)
            pathshard = self._shardspace_(topic, **shard).root
            kvhandle_pathshard_list.append((kvhandle, pathshard))
        intent_datapage = {kvhandle: pathshard for kvhandle, pathshard in kvhandle_pathshard_list}
        return intent_datapage

    @OVERRIDE
    #RENAME: -> _shard_page_extent_valid_?
    def _extent_shard_valid_(self, topic, **tagshardscope):
        pathset = self._shardspace_(topic, **tagshardscope).root
        valid = self.versionspace.filesystem.isdir(pathset)
        return valid

    #RENAME: -> block_page_extent()
    def extent_datapage(self, topic, **scope):
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        intent_datapage = self.intent_datapage(topic, **scope)
        extent_datapage = {}
        for kvhandle, shard_pathset in intent_datapage.items():
            shardscope = self._kvchain_to_scope_(topic, kvhandle)
            valid = self._extent_shard_valid_(topic, **shardscope)
            if valid:
                extent_datapage[kvhandle] = shard_pathset
        return extent_datapage

    #RENAME: -> block_page_shortfall
    def shortfall_datapage(self, topic, **scope):
        intent_datapage = self.intent_datapage(topic, **scope)
        extent_datapage = self.extent_datapage(topic, **scope)
        shortfall_datapage = {}
        for kvhandle, intent_pathshard in intent_datapage.items():
            if isinstance(intent_pathshard, str):
                if kvhandle not in extent_datapage or extent_datapage[kvhandle] != intent_pathshard:
                    shortfall_pathshard = intent_pathshard
                else:
                    shortfall_pathshard = []
            else:
                if kvhandle not in extent_datapage:
                    shortfall_pathshard = intent_pathshard
                else:
                    extent_pathshard = extent_datapage[kvhandle]
                    shortfall_pathshard = [intent_filepath for intent_filepath in intent_pathshard
                                             if intent_filepath not in extent_pathshard]
            if len(shortfall_pathshard) == 0:
                continue
            shortfall_datapage[kvhandle] = shortfall_pathshard
        return shortfall_datapage

    #REMOVE?
    def intent_databook(self, **scope):
        return self._page_databook("intent", **scope)

    #REMOVE?
    def extent_databook(self, **scope):
        return self._page_databook("extent", **scope)

    #RENAME: -> collate_pages
    @staticmethod
    def collate_datapages(topic, *datapages):
        # Each datapage is a dict {kvhandle -> filepathset}.
        # `collate` is just a union of dicts.
        # Assuming each kvhandle exists only once in datapages or only the last occurrence matters.
        collated_datapage = {kvhandle: filepathset for datapage in datapages for kvhandle, filepathset in datapage.items()}
        return collated_datapage

    #RENAME: -> collate_books
    @staticmethod
    def collate_databooks(*databooks):
        # Collate all pages from all books within a topic
        topic_datapages = {}
        for databook in databooks:
            for topic, datapage in databook.items():
                if topic in topic_datapages:
                    topic_datapages[topic].append(datapage)
                else:
                    topic_datapages[topic] = [datapage]
        collated_databook = \
            {topic: Databook.collate_datapages(topic, *datapages) \
             for topic, datapages in topic_datapages.items()}
        return collated_databook

    #REMOVE?
    def intent(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        intent_databook = self.intent_databook(**blockscope)
        _intent_databook = self._databook_kvchain_to_scope(intent_databook)
        return _intent_databook

    #REMOVE?
    def extent(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        extent_databook = self.extent_databook(**blockscope)
        extent = self._databook_kvchain_to_scope(extent_databook)
        return extent

    def shortfall(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        shortfall_databook = self.shortfall_databook(**blockscope)
        _shortfall_databook = self._databook_kvchain_to_scope(shortfall_databook)
        return _shortfall_databook

    def _extent_shard_metric_(self, topic, **shardscope):
        extent_datapage = self.extent_datapage(topic, **shardscope)
        if len(extent_datapage) != 1:
            raise ValueError(f"Too many shards in extent_datapage: {extent_datapage}")
        pathset = list(extent_datapage.values())[0]
        if isinstance(pathset, str):
            metric = 1
        else:
            metric = len(pathset)
        return metric
    
    #REMOVE?
    #TODO: factor into extent_datapage_metric, extent_databook_metric and extent_metric?
    def extent_metric(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        extent = self.extent(**blockscope)
        extent_metric_databook = {}
        for topic, scope_pathset in extent.items():
            if len(scope_pathset) == 2:
                scope, _ = scope_pathset
                shard_metric = self._extent_shard_metric_(topic, **blockscope)
            else:
                shard_metric = None
            if topic not in extent_metric_databook:
                extent_metric_databook[topic] = []
            extent_metric_databook[topic].append((scope, {'metric': shard_metric}))
        return extent_metric_databook

    #REMOVE: fold into block_shortfall?
    def shortfall_databook(self, **scope):
        blockscope = self._blockscope_(**scope)
        pagekvhandles_list = [set(self.shortfall_datapage(topic, **blockscope).keys()) for topic in self.topics]
        bookkvhandleset = set().union(*pagekvhandles_list)
        shortfall_databook = {}
        for topic in self.topics:
            shortfall_datapage = {}
            for kvhandle in bookkvhandleset:
                scope = {key: val for key, val in kvhandle}
                filepathset = self._shardspace_(topic, **blockscope).root
                shortfall_datapage[kvhandle] = filepathset
            shortfall_databook[topic] = shortfall_datapage
        return shortfall_databook
    
    @OVERRIDE
    def _shardspace_(self, topic, **shard):
        tagshard = self._tagscope_(**shard)
        hvchain = self._scope_to_hvchain_(topic, **tagshard)
        subspace = self.versionspace.subspace(*hvchain).ensure()
        return subspace

    def _scope_to_hvchain_(self, topic, **shard):
        if topic is not None:
            shardhivechain = [topic]
        else:
            shardhivechain = []
        for key in self.shard_keys:
            shardhivechain.append(f"{key}={shard[key]}")
        return shardhivechain

    def _scope_to_kvchain_(self, topic, **scope):
        _kvhandle = tuple((key, scope[key]) for key in self.shard_keys)
        kvhandle = KeyValHandle(_kvhandle)
        return kvhandle

    def _kvchain_to_scope_(self, topic, kvchain):
        return {key: val for key, val in kvchain}

    def _kvchain_to_hvchain_(self, topic, kvchain):
        scope = self._kvchain_to_scope_(topic, kvchain)
        hivechain = self._scope_to_hvchain_(topic, **scope)
        return hivechain

    def _kvchain_to_filename(self, topic, kvchain, extension=None):
        datachain = self._kvchain_to_hvchain_(topic, kvchain)
        name = '.'.join(datachain)
        filename = name if extension is None else name+'.'+extension
        return filename

    def _kvchain_to_dirpath(self, topic, kvchain):
        datachain = self._kvchain_to_hvchain_(topic, kvchain)
        subspace = self.versionspace.subspace(*datachain)
        path = subspace.root
        return path
    
    def _databook_kvchain_to_scope(self, databook):
        _databook = {}
        for topic, datapage in databook.items():
            if not topic in _databook:
                _databook[topic] = ()
            for kvhandle, pathset in datapage.items():
                scope = self._kvchain_to_scope_(topic, kvhandle)
                _databook[topic] += (scope, pathset)
        return _databook

    def _lock_kvchain(self, topic, kvchain):
        if self.lock_pages:
            hivechain = self._kvchain_to_hvchain_(topic, kvchain)
            self.versionspace.subspace(*hivechain).acquire()

    def _unlock_kvchain(self, topic, kvchain):
        if self.lock_pages:
            hivechain = self._kvchain_to_hvchain_(topic, kvchain)
            self.versionspace.subspace(*hivechain).release()

    def _recordspace_(self):
        subspace = self.anchorspace.subspace('.records')
        return subspace

    #REMOVE: unroll inplace where necessary
    def _page_databook(self, domain, **kwargs):
        # This databook is computed by computing a page for each topic separately, 
        # via a dedicated function call with topic as an arg, using a domain-specific
        # method.  domain: 'intent'|'extent'|'shortfall'
        _datapage = getattr(self, f"{domain}_datapage")
        databook = {topic: _datapage(topic, **kwargs) for topic in self.topics}
        return databook

    #REMOVE: -> DBX.show_metric?
    def print_metric(self, **scope):
        metric = self.extent_metric(**scope)
        print(metric)
    
    BUILD_RECORDS_COLUMNS_SHORT = ['stage', 'version', 'scope', 'alias', 'task_id', 'metric', 'status', 'date', 'timestamp', 'runtime_secs']

    def show_build_records(self, *, print=False, full=False, columns=None, all=False, tail=5):
        """
        All build records for a given Databook class, irrespective of alias and version (see `list()` for more specific).
        'all=True' forces 'tail=None'
        """
        short = not full
        recordspace = self._recordspace_()

        frame = None
        try:
            parquet_dataset = pq.ParquetDataset(recordspace.root, use_legacy_dataset=False, filesystem=recordspace.filesystem)
            table = parquet_dataset.read()
            frame = table.to_pandas()
        except:
            pass
    
        if frame is not None and len(frame) > 0:
            
            frame.reset_index(inplace=True, drop=True)
            if columns is None:
                _columns = frame.columns
            else:
                _columns = [c for c in columns if c in frame.columns]
            if short:
                _columns_ = [c for c in _columns if c in Databook.BUILD_RECORDS_COLUMNS_SHORT]
            else:
                _columns_ = _columns
            frame.sort_values(['timestamp'], inplace=True)
            _frame = frame[_columns_]
        else:
            _frame = pd.DataFrame()

        if all:
            tail = None
        if tail is not None:
            __frame = _frame.tail(tail)
        else:
            __frame = _frame
        
        if print:
            __builtins__['print'](__frame)
        return __frame

    def show_build_record_columns(self, *, full=True, **ignored):
        frame = self.show_build_records(full=full)
        columns = frame.columns
        return columns
    
    def show_build_record(self, *, record=None, full=False):
        import datablocks
        try:
            records = self.show_build_records(full=full)
        except:
            return None
        if len(records) == 0:
            return None
        if isinstance(record, int):
            _record = records.loc[record]
        if record is None:
            _record = records.iloc[-1]
        return _record

    def show_build_graph(self, *, record=None, node=tuple(), show=('logpath', 'logpath_status', 'exception'), _show=tuple(), **kwargs):
        _record = self.show_build_record(record=record, full=True)
        _summary = _record['report_summary']
        if isinstance(show, str):
            show=(show,)
        if isinstance(_show, str):
            _show=(_show,)
        show = show + _show
        _graph = Graph(_summary, show=show, **kwargs)
        if node is not None:
            graph = _graph.node(*node)
        else:
            graph = _graph
        return graph
    
    def show_build_batch_count(self, *, record=None, print=True):
        g = self.show_build_graph(record=record, node=(1,)) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        nbatches = len(g.args)
        if print:
            _print(nbatches)
        else:
            return nbatches
    
    def show_build_batch_graph(self, *, record=None, batch=0, **kwargs):
        g = self.show_build_graph(record=record, node=(1,batch), **kwargs) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        return g

    def show_build_batch_log(self, *, record=None, batch=0, **kwargs):
        g = self.show_build_graph(record=record, node=(1,batch), **kwargs) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        log = g.log()
        return log
    
    def show_build_batch_logpath(self, *, record=None, batch=0):
        g = self.show_build_graph(record=record, node=(1,batch)) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        logpath = g.logpath()
        return logpath
    
    def show_build_summary(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        summary = _record['report_summary']
        return summary

    def show_build_scope(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        scopestr = _record['scope']
        scope = _eval(scopestr)
        return scope

    def show_build_logspace(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        _logspace = _record['logspace']
        logspace = _eval(_logspace)
        return logspace
    
    def show_build_logname(self, *, record=None, **kwargs):
        record = self.show_build_record(record=record, full=True)
        _ = record['logname']
        return _
    
    def show_build_logpath(self, *, record=None, **kwargs):
        logspace = self.show_build_logspace(record=record)
        logname = self.show_build_logname(record=record)
        if logspace and logname:
            logpath = logspace.join(logspace.root, logname+'.log')
            return logpath

    @DEPRECATED # USE show_build_graph()
    def show_build_logs(self, *, record=None, request_max_len=50, **kwargs):
        g = self.show_build_graph(record=record, **kwargs)
        if g is not None:
            _ = g.validate_logs(request_max_len=request_max_len)
            return _
        
    @DEPRECATED # USE show_build_graph()
    def show_build_request(self, *, record=None, max_len=None, **kwargs):
        kwargs[f'request_max_len'] = max_len
        g = self.show_build_graph(record=record, **kwargs)
        # Build graph looks like this: collate({extent}, collate(build_shard, ...)) 
        # and we want the outer collate's arg 1 (inner collate)'s arg 0 -- build_shard 
        if g is None:
            return None
        _ = g.request
        return _
    
    @DEPRECATED # Use show_build_graph()
    def show_build_result(self, *, record=None, max_len=None, **kwargs):
        kwargs[f'request_max_len'] = max_len
        g = self.show_build_graph(record=record, **kwargs)
        # Build graph looks like this: collate({extent}, collate(build_shard, ...)) 
        # and we want the outer collate's arg 1 (inner collate)'s arg 0 -- build_shard 
        if g is None:
            return None
        _ = g.result
        return _

    def show_build_log(self, *, record=None, node=None, **kwargs):
        g = self.show_build_graph(record=record, node=None, **kwargs)
        if g is not None:
            _ = g.log()
            return _

    def _build_databook_request_lifecycle_callback_(self, **blockscope):
        #tagscope = self._tagscope_(**blockscope)
        classname = ctor_name(self.__class__)
        versionstr = repr(self.version)
        #tagscopestr = repr(tagscope)
        namestr = f"{classname}:{versionstr}(**{blockscope})"
        hashstr = hashlib.sha256(namestr.encode()).hexdigest()[:10]
        alias = self.alias
        if alias is None:
            alias = hashstr
        recordspace = self._recordspace_().ensure()

        def _write_record_lifecycle_callback(lifecycle_stage, request, response):
            # TODO: report_summary should be just a repr(report.to_dict()), ideally, repr(report)
            # TODO: however, we may rewrite report.result
            task = request.task
            timestamp = int(microseconds_since_epoch())
            datestr = datetime_to_microsecond_str()
            blockscopestr = repr(blockscope)
            _record = dict(record_schema=Databook.record_schema,
                           alias=alias,
                           stage=lifecycle_stage.name,
                           classname=classname,
                            version=versionstr,
                            scope=blockscopestr,
                            date=datestr,
                            timestamp=timestamp,
                            runtime_secs='',
                            key=str(task.key),
                            id=str(task.id),
                            logspace=str(task.logspace),
                            logname=str(task.logname),
                            status='',
                            success='',
                            metric='',
                            report_summary='',
            )
        
            if self.verbose:
                    print(f"[BUILD] writing record for request: {request}")
                    print(f"[BUILD] topics: {self.topics}")
                    print(f"[BUILD] blockscope: {blockscope}")
            
            logname = None
            task_id = None
            if response is not None:
                if response.done_time is not None and response.start_time is not None:
                    runtime_secs = (response.done_time - response.start_time).total_seconds()
                else:
                    runtime_secs = str(None)
                report = response.report()
                task_id = response.id
                report_summary = report.summary()
                #args_reports = report.args_reports
                #kwargs_reports = report.kwargs_reports
                #args_results = [arg_report.result if isinstance(arg_report, Report) else arg_report for arg_report in args_reports]
                #kwargs_results = {key: arg_report.result if isinstance(arg_report, Report) else arg_report for key, arg_report in kwargs_reports.items()}
                logspace=response.logspace
                logpath = report.logpath
                if logpath is not None:
                    _, logname_ext = os.path.split(logpath)
                    logname, ext = logname_ext.split('.')
                summarystr = repr(report_summary)
                """
                if self.verbose:
                    print(f"[BUILD] summary: {summarystr}, logpath: {logpath}, logname: {logname}")
                """
                logging.debug(f"[BUILD] summary: {summarystr}, logpath: {logpath}, logname: {logname}")
                _record.update(dict(
                                    task_id=str(task_id),
                                    runtime_secs=f"{runtime_secs}",
                                    status=report_summary['status'],
                                    success=report_summary['success'],
                                    logspace=repr(logspace),
                                    logname=logname,
                                    report_summary=summarystr,
                            ))
            records = []
            shard_list = self.scope_to_shards(**blockscope)
            for shard in shard_list:
                for topic in self.topics:
                    record = copy.deepcopy(_record)
                    if response is not None:
                        metric = self._extent_shard_metric_(topic, **shard)
                        record['metric'] = str(metric)
                    record['topic'] = topic
                    record['shardscope'] = repr(shard)
                    records.append(record)
            record_frame = pd.DataFrame.from_records(records)
            record_frame.index.name = 'index'
            record_filepath = \
                recordspace.join(recordspace.root, f"alias-{alias}-stage-{lifecycle_stage.name}-task_id-{task_id}-datetime-{datestr}.parquet")
            if self.verbose:
                print(f"[BUILD] Writing build record at lifecycle_stage {lifecycle_stage.name} to {record_filepath}")
            logging.debug(f"[BUILD] Writing build record at lifecycle_stage {lifecycle_stage.name} to {record_filepath}")
            record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)
        return _write_record_lifecycle_callback

    def build_request(self, **scope):
        request = Request(self.build, **scope).apply(self.pool)
        return request

    def build(self, **scope):
        def equal_scopes(s1, s2):
            if set(s1.keys()) != (s2.keys()):
                return False
            for key in s1.keys():
                if s1[key] != s2[key]:
                    return False
            return True
        # TODO: consistency check: sha256 alias must be unique for a given version or match scope
        blockscope = self._blockscope_(**scope)
        if self.show_build_record() is not None:
            _scope = self.show_build_scope()
            if not equal_scopes(blockscope, _scope):
                raise ValueError(f"Attempt to overwrite prior scope {_scope} with {blockscope} for {self.__class__} alias {self.alias}")
        """
        pool_key = utils.datetime_now_key()
        pool_dataspace = self.versionspace.subspace(*(self.pool.anchorchain+(pool_key,))).ensure()
        pool = self.pool.clone(dataspace=pool_dataspace)
        """
        request = self.build_databook_request(**blockscope).with_lifecycle_callback(self._build_databook_request_lifecycle_callback_(**blockscope))
        response = request.evaluate()
        if self.verbose:
            print(f"task_id: {response.id}")
        result = response.result()
        return result

    # TODO: build_databook_* -> build_block_*?
    @OVERRIDE
    def build_databook_request(self, **scope):
        blockscope = self._blockscope_(**scope)
        if self.reload:
            shortfall_databook = self.intent_databook(**blockscope)
        else:
            shortfall_databook = self.shortfall_databook(**blockscope)
        shortfall_databook_kvhandles_lists = [list(shortfall_databook[topic].keys()) for topic in self.topics]
        shortfall_databook_kvhandles_list = [_ for __ in shortfall_databook_kvhandles_lists for _ in __]
        shortfall_databook_kvhandles = list(set(shortfall_databook_kvhandles_list))

        shortfall_batchscope_list = \
            self._kvchains2batches_(*shortfall_databook_kvhandles)
        shortfall_batchscope_list = \
            [{k: blockscope[k] for k in tscope.keys()} for tscope in shortfall_batchscope_list]
        logger.debug(f"Requesting build of shortfall_tagbatch_list: {shortfall_batchscope_list}")
        shortfall_batch_requests = \
            [self._build_batch_request_(self._tagscope_(**shortfall_batchscope_list[i]), shortfall_batchscope_list[i])
                            .apply(self.pool) for i in range(len(shortfall_batchscope_list))]
        shortfall_batch_requests_str = "[" + \
                                          ", ".join(str(_) for _ in shortfall_batch_requests) + \
                                          "]"
        logger.debug(f"shortfall_batch_requests: " + shortfall_batch_requests_str)
        collated_shortfall_batch_request = \
            Request(self.collate_databooks, *shortfall_batch_requests)
        logger.debug(f"collated_shortfall_batch_request: {collated_shortfall_batch_request}")
        extent_databook = self.extent_databook(**blockscope)
        build_databook_request = \
            Request(self.collate_databooks, extent_databook, collated_shortfall_batch_request)
        return build_databook_request

    @OVERRIDE
    # tagscope is necessary since batchscope will be expanded before being passed to _build_batch_
    def _build_batch_request_(self, tagscope, batchscope):
        self.versionspace.ensure()
        return Request(self._build_batch_, tagscope, batchscope)

    @OVERRIDE
    def _build_batch_(self, tagscope, batchscope):
        raise NotImplementedError()

    def read(self, topic=None, **blockscope):
        _blockscope = self._blockscope_(**blockscope)
        request = self.read_databook_request(topic, **_blockscope)
        response = request.evaluate()
        result = response.result()
        return result

    @OVERRIDE
    # tagbatchscope is necessary since batchscope will be expanded before being passed to _read_block_
    def read_databook_request(self, topic, **blockscope):
        tagscope = self._tagscope_(**blockscope)
        request = Request(self._read_block_, tagscope, topic, **blockscope)
        return request
    
    @OVERRIDE
    def _read_block_(self, tagscope, topic, blockscope):
        raise NotImplementedError()
    
    def UNSAFE_clear_records(self):
        recordspace = self._recordspace_()
        recordspace.remove()
    
    def UNSAFE_clear(self, **scope):
        self.UNSAFE_clear_records()
        blockscope = self._blockscope_(**scope)
        tagblockscope = self._tagscope_(**blockscope)
        request = self.UNSAFE_clear_request(**tagblockscope)
        _ = request.compute()
        return _

    @OVERRIDE
    def UNSAFE_clear_request(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagblockscope = self._tagscope_(**blockscope)
        request = Request(self._UNSAFE_clear_block_, **blockscope)
        return request
    
    @OVERRIDE
    def _UNSAFE_clear_block_(self, **scope):
        for topic in self.topics:
            shardspace = self._shardspace_(topic, **scope)
            """
            if self.verbose:
                print(f"Clearing shardspace {shardspace}")
            """
            logging.debug(f"Clearing shardspace {shardspace}")
            shardspace.remove()




class DBX(request.Proxy):
    """
        DBX instantiates a Databook class on demand, in particular, different instances depending on the build(**kwargs),
            . pool=Ray()
            . etc
        Not inheriting from Databook also has the advantage of hiding the varable scope API that Databook.build/read/etc presents.
        DBX is by definition a fixed scope block.
    """
    
    @staticmethod
    def show_datablocks(*, dataspace=DATABLOCKS_DATALAKE, pretty_print=True):
        def _chase_anchors(_dataspace, _anchorchain=()):
            filenames = _dataspace.list()
            anchorchains = []
            for filename in filenames:
                if filename.startswith('.'):
                    continue
                elif dataspace.isfile(filename):
                    continue
                elif filename.startswith('version='):
                    anchorchain = _anchorchain + (filename,)
                    anchorchains.append(anchorchain)
                else:
                    dataspace_ = _dataspace.subspace(filename)
                    anchorchain_ = _anchorchain+(filename,)
                    _anchorchains = _chase_anchors(dataspace_, anchorchain_)
                    anchorchains.extend(_anchorchains)
            return anchorchains
        
        datablock_dataspace = dataspace.subspace(DBX_PREFIX)

        anchorchains = _chase_anchors(datablock_dataspace)
        datablocks = {'.'.join(anchorchain[:-1]): anchorchain[-1] for anchorchain in anchorchains}
        if pretty_print:
                for key, value in datablocks.items():
                    print(f"{key}: {value}")
        else:
            return datablocks
    
    def __init__(self, 
                datablock_cls_or_clstr, 
                alias=None,
                **datablock_kwargs,):
        self.alias = alias
        #DBX_dataspace = dataspace.subspace(DBX_PREFIX)
        if isinstance(datablock_cls_or_clstr, str):
            self.datablock_clstr = datablock_cls_or_clstr
            datablock_clstrparts = self.datablock_clstr.split('.')
            if len(datablock_clstrparts) == 1:
                self.datablock_module_name = __name__
                self.datablock_clsname = datablock_clstrparts[0]
            else:
                self.datablock_module_name = '.'.join(datablock_clstrparts[:-1])
                self.datablock_clsname = datablock_clstrparts[-1]
            mod = importlib.import_module(self.datablock_module_name)
            self.datablock_cls = getattr(mod, self.datablock_clsname)
        else:
            self.datablock_cls = datablock_cls_or_clstr
            self.datablock_module_name = self.datablock_cls.__module__
            self.datablock_clstr = f"{self.datablock_module_name}.{self.datablock_cls.__name__}"
        self.datablock_kwargs = datablock_kwargs

        self._scope = {} # initialize to default scope
        @functools.wraps(self.datablock_cls.SCOPE)
        def update_scope(**scope):
            self._scope.update(**scope)
            return self
        self.SCOPE = update_scope
        # scope is extracted via a property, which validates the scope

        self.databook_kwargs = dict(
            dataspace=DATABLOCKS_DATALAKE,
            pool=DATABLOCKS_LOGGING_REDIRECT_POOL,
            tmpspace=None, # derive from dataspace?
            lock_pages=False,
            throw=True, # fold into `pool`?
            rebuild=False, # move to build()?
            verbose=False,
        )
        @functools.wraps(Databook)
        def update_databook_kwargs(**kwargs):
            self.databook_kwargs.update(**kwargs)
            return self
        self.Databook = update_databook_kwargs

    def __repr__(self):
        """
        if self.alias is not None:
            _ =  Tagger().repr_func(self.__class__, self.datablock_clstr, self.alias)
        else:
            _ = Tagger().repr_func(self.__class__, self.datablock_clstr)
        """
        #FIX: do not make default self.alias None explicit
        _ =  Tagger().repr_func(self.__class__, self.datablock_clstr, self.alias)
        return _

    # TODO: spell out `.DATABOOK()` and `.SCOPE()` modifications?
    def __tag__(self):
        tag = f"{self.datablock_clstr}"
        if self.alias is not None:
            tag += f"@{self.alias}"
        return tag

    def __hash__(self):
        _repr = self.__tag__()
        _ =  int(hashlib.sha1(_repr.encode()).hexdigest(), 16)
        return _
    
    def __getattr__(self, attr):
        return getattr(self.databook, attr)

    @property
    def databook(self):
        databook_cls = self._make_databook_class()
        databook = databook_cls(self.alias, **self.databook_kwargs)
        return databook

    def scope(self):
        # TODO: _records --> (journal)_entries
        records = self.databook.show_build_records()
        if len(records) == 0:
            msg = f"No journal entries for databook {self.databook} of version: {self.databook.version}"
            if self.verbose:
                print(msg)
            _scope = self._scope
        else:
            if self.databook.alias is None:
                recs = records
            else:
                recs = records[records.alias == self.databook.alias]
            if len(recs) == 0:
                msg = f"No journal entries for datablock {self.databook} of version: {self.version} and alias {self.alias}"
                if self.verbose:
                    print(msg)
                _scope = self._scope
            else:
                rec = recs.iloc[-1]
                # TODO?: fix serialization of version to record to remove `repr`
                if rec['version'] != repr(self.databook.version) and rec['version']:
                    msg = f"Version mismatch for databook {self.databook} of version: {self.databook.version} and journal entry with alias {self.databook.alias}: {rec['version']}"
                    if self.verbose:
                        print(msg)
                    raise ValueError(msg)
                _scope = _eval(rec['scope'])
        return _scope
    
    @property
    def request(self):
        """
            For request.Proxy protocol.
        """
        return self.read_request()

    def build_request(self):
        build_request = self.databook.build_request(**self.scope())
        return build_request

    def build(self):
        build_request = self.build_request()
        result = build_request.compute()
        return result

    def read_request(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"{self} of version {self.version} has not been built yet")
        request = self.databook.read_databook_request(topic, **self.scope())\
            .set(summary=lambda _: self.extent()[topic])
        return request

    def reader(self, topic=DEFAULT_TOPIC):
        reader = self.read_request(topic)
        return reader
    
    def read(self, topic=DEFAULT_TOPIC):
        read_request = self.read_request(topic)
        result = read_request.compute()
        return result
    
    def extent(self, topic=None):
        _ = self.databook.extent(topic, **self.scope())
        return _
    
    def extent_metric(self, topic=None):
        _ = self.databook.extent_metric(self, topic, **self.scope())
        return _    
    
    def _validate_subscope(self, **subscope):
        #TODO: check that subscope is a subscope of self.scope
        return subscope
    
    def _make_databook_class(dbx):
        """
            Using 'dbx' instead of 'self' here to avoid confusion of the meaning of 'self' in different scopes: as a DBX instance and a Databook subclass instance.
            This Databook subclass factory using `datablock_cls` as implementation of the basic `build()`, `read()`, `valid()`, `metric()` methods.
            >. databook gets block_to_shard_keys and batch_to_shard_keys according to datablock_scope and datablock_cls.SCOPE RANGE annotations.
        """ 
        def __init__(self, *args, **kwargs):
            Databook.__init__(self, *args, **kwargs)
            self._datablock = None

        @property
        def __datablock(self):
            if self._datablock is None:
                self._datablock = self.datablock_cls(**self.datablock_kwargs)
            return self._datablock

        def __datablock_shardroots__(self, topics=None, **tagscope):
            if topics is None:
                topics = self.topics
            shardscope_list = self.scope_to_shards(**tagscope)
            shardspace_list = [self._shardspace_(topic, **shardscope) for topic in topics for shardscope in shardscope_list]
            shardspace_roots = [shardspace.root for shardspace in shardspace_list]
            return shardspace_roots

        def __build_batch__(self, tagscope, batchscope):
            datablock_batchscope = self.datablock_cls.SCOPE(**batchscope)
            datablock_shard_roots = self.datablock_shardroots(**tagscope)
            self.datablock.build(datablock_batchscope, self.dataspace.filesystem, *datablock_shard_roots)
            _ = self.extent_databook(**tagscope)
            return _

        def __read_block__(self, tagscope, topic, **blockscope):
            datablock_blockscope = self.datablock_cls.SCOPE(**blockscope)
            datablock_shard_roots = self.datablock_shardroots(topics=[topic], **tagscope)
            if topic == DEFAULT_TOPIC and not hasattr(self.datablock, 'TOPICS') and not hasattr(self.datablock, 'topics'):
                _ = self.datablock.read(datablock_blockscope, self.dataspace.filesystem, *datablock_shard_roots)
            else:
                _ = self.datablock.read(topic, datablock_blockscope, self.dataspace.filesystem, *datablock_shard_roots)
            return _

        def __extent_shard_valid__(self, topic, **tagshardscope):
            datablock_tagshardscope = self.datablock_cls.SCOPE(**tagshardscope)
            datablock_shard_roots = self.datablock_shardroots(topics=[topic], **tagshardscope)
            if topic == DEFAULT_TOPIC and not hasattr(self.datablock, 'TOPICS') and not hasattr(self.datablock, 'topics'):
                _ = self.datablock.valid(datablock_tagshardscope, self.dataspace.filesystem, *datablock_shard_roots)
            else:
                _ = self.datablock.valid(topic, datablock_tagshardscope, self.dataspace.filesystem, *datablock_shard_roots)
            return _
        
        def __extent_shard_metric__(self, topic, **tagshardscope):
            datablock_tagshardscope = self.datablock_cls.SCOPE(**tagshardscope)
            datablock_shard_roots = self.datablock_shardroots(topics=[topic], **tagshardscope)
            if topic == DEFAULT_TOPIC and not hasattr(self.datablock, 'TOPICS') and not hasattr(self.datablock, 'topics'):
                _ = self.datablock.metric(datablock_tagshardscope, self.dataspace.filesystem, *datablock_shard_roots)
            else:
                _ = self.datablock.metric(topic, datablock_tagshardscope, self.dataspace.filesystem, *datablock_shard_roots)
            return _

        scope_fields = dataclasses.fields(dbx.datablock_cls.SCOPE)
        scope_field_names = [field.name for field in scope_fields]
        __block_keys = scope_field_names
        __block_defaults = {field.name: field.default  for field in scope_fields if field.default is not None}
        __batch_to_shard_keys = {field.name: field.name for field in scope_fields if isinstance(field.type, RANGE)}

        '''
        try:
            importlib.import_module(databook_module_name)
        except:
            spec = importlib.machinery.ModuleSpec(databook_module_name, None)
            mod = importlib.util.module_from_spec(spec)
        '''

        __module_name = DBX_PREFIX + "." + dbx.datablock_module_name
        __cls_name = DBX_PREFIX + "." + dbx.datablock_clstr
        __datablock_cls = dbx.datablock_cls
        __datablock_kwargs = dbx.datablock_kwargs
        if hasattr(dbx.datablock_cls, 'TOPICS'):
            __topics = dbx.datablock_cls.TOPICS
        else:
            __topics = [DEFAULT_TOPIC]
        if hasattr(dbx.datablock_cls, 'VERSION'):
            __version = dbx.datablock_cls.VERSION
        else:
            __version = DEFAULT_VERSION
        databook_classdict = {
                    '__module__': __module_name,
                    'block_keys': __block_keys, 
                    'block_defaults': __block_defaults,
                    'batch_to_shard_keys': __batch_to_shard_keys,
                    '__init__': __init__,
                    'version': __version,
                    'topics': __topics,
                    'datablock_cls': __datablock_cls,
                    'datablock_kwargs': __datablock_kwargs,
                    'datablock': __datablock,
                    'datablock_shardroots': __datablock_shardroots__,
                    '_build_batch_': __build_batch__,
                    '_read_block_':  __read_block__,
        }
        if hasattr(dbx.datablock_cls, 'valid'):
            databook_classdict['_extent_shard_valid_'] = __extent_shard_valid__
        if hasattr(dbx.datablock_cls, 'metric'):
            databook_classdict['_extent_shard_metric_'] = __extent_shard_metric__

        databook_class = type(__cls_name, 
                               (Databook,), 
                               databook_classdict,)
        return databook_class
    
    