import collections
from collections.abc import Callable, Iterable
import copy
import dataclasses
from dataclasses import dataclass
import functools
import hashlib
import importlib
import logging
import math
import os
import pdb
import traceback

from typing import Any, TypeVar, Generic, Tuple, Union, List, Dict

import fsspec


import pyarrow.parquet as pq
import pandas as pd


from . import signature
from .signature import Signature, ctor_name
from .signature import tag, Tagger
from .utils import DEPRECATED, OVERRIDE, microseconds_since_epoch, datetime_to_microsecond_str
from .eval import request
from .eval.request import Request, Report, LAST, NONE, Graph
from .eval.pool import DATABLOCKS_STDOUT_LOGGING_POOL, DATABLOCKS_FILE_LOGGING_POOL
from .dataspace import Dataspace, DATABLOCKS_DATALAKE


def _eval(string):
    """
        Eval in the context of impoirted datablocks.datablock.
    """
    import datablocks.datablock
    _eval = __builtins__['eval']
    _ = _eval(string)
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
        def _scope_to_kwargs(scope, plural_key_counters):
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
                raise ValueError(f"Unknown key '{key}' is not in databuilder keys {list(self.shard_keys)} "
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
            kwargs = _scope_to_kwargs(_scope, plural_key_counters)
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

    # TODO: --> _handles_to_batches_?
    def _kvhandles_to_batches_(self, *kvhandles):
        # We assume that all keys are always the same -- equal to self.keys
        # Nontrivial grouping of kwargs into batches is possible only when cls.batch_by_plural_key is in cls.keys
        if len(kvhandles) == 0:
            batch_list = []
        else:
            vals_list = [tuple(val for _, val in kvhandle) for kvhandle in kvhandles]
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


class KVHandle(tuple):
    def __call__(self, *args, **kwargs) -> Any:
        return super().__call__(*args, **kwargs)
    
    def __str__(self):
        dict_ = {key: val for key, val in self}
        str_ = f"{dict_}"
        return str_


DEFAULT_TOPIC = None
DEFAULT_VERSION = '0.0.0'

class Databuilder(Anchored, Scoped): 
    # TODO: implement support for multiple batch_to_shard_keys
    version = DEFAULT_VERSION
    topics = [DEFAULT_TOPIC]
    signature = Signature((), ('dataspace', 'version',)) # extract these attrs and use in __tag__

    RECORD_SCHEMA_VERSION = '0.2.0'

    # TODO: make dataspace, version position-only and adjust Signature
    def __init__(self,
                 alias=None,
                 *,
                 dataspace=DATABLOCKS_DATALAKE,
                 tmpspace=None,
                 lock_pages=False,
                 throw=True,
                 rebuild=False,
                 verbose=False,
                 debug=False,
                 pool=DATABLOCKS_STDOUT_LOGGING_POOL):
        Anchored.__init__(self)
        Scoped.__init__(self)
        self.alias = alias
        self.dataspace = dataspace
        self._tmpspace = tmpspace
        self.anchorspace = dataspace.subspace(*self.anchorchain)
        self.versionspace = self.anchorspace.subspace(f"version={str(self.version)}",)
        self.lock_pages = lock_pages
        self.verbose = verbose
        self.debug = debug
        self.pool = pool
        self.reload = rebuild
        self.throw = throw
        if self.lock_pages:
            raise NotImplementedError("self.lock_pages not implemented for Databuilder")
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
        #DEBUG
        #print(f"Databuilder: -------> self.topics: {self.topics}")

    def __repr__(self):
        return self.__tag__()

    def __tag__(self):
        if hasattr(self, 'signature'):
            schema = self.signature.schema
            argattrs = schema[0]
            args = [getattr(self, attr) for attr in argattrs]
            kwargattrs = schema[1]
            kwargs = {attr: getattr(self, attr) for attr in kwargattrs}
            repr = Tagger().tag_ctor(self.__class__, *args, **kwargs)
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
            kvhandle = self._scope_to_kvhandle_(topic, **shard)
            pathshard = self._shardspace_(topic, **shard).root
            kvhandle_pathshard_list.append((kvhandle, pathshard))
        intent_datapage = {kvhandle: pathshard for kvhandle, pathshard in kvhandle_pathshard_list}
        return intent_datapage

    @OVERRIDE
    #RENAME: -> _shard_page_extent_valid_?
    def _extent_shard_valid_(self, topic, tagshardscope):
        pathset = self._shardspace_(topic, **tagshardscope).root
        valid = self.versionspace.filesystem.isdir(pathset)
        if self.debug:
            if valid:
                print(f"_extent_shard_valid_: VALID: shard with topic {repr(topic)} with scope with tag {tagshardscope}")
            else:
                print(f"_extent_shard_valid_: INVALID shard with topic {repr(topic)} with scope with tag {tagshardscope}")
        return valid

    #RENAME: -> block_page_extent()
    def extent_datapage(self, topic, **scope):
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        intent_datapage = self.intent_datapage(topic, **scope)
        extent_datapage = {}
        for kvhandle, shard_pathset in intent_datapage.items():
            shardscope = self._kvhandle_to_scope_(topic, kvhandle)
            valid = self._extent_shard_valid_(topic, shardscope)
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
            {topic: Databuilder.collate_datapages(topic, *datapages) \
             for topic, datapages in topic_datapages.items()}
        return collated_databook

    #REMOVE?
    def intent(self, **scope):
        blockscope = self._blockscope_(**scope)
        tagscope = self._tagscope_(**blockscope)
        intent_databook = self.intent_databook(**tagscope)
        _intent_databook = self._databook_kvhandle_to_scope(intent_databook)
        return _intent_databook

    #REMOVE?
    def extent(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        extent_databook = self.extent_databook(**blockscope)
        extent = self._databook_kvhandle_to_scope(extent_databook)
        return extent

    def shortfall(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        shortfall_databook = self.shortfall_databook(**blockscope)
        _shortfall_databook = self._databook_kvhandle_to_scope(shortfall_databook)
        return _shortfall_databook

    def _extent_shard_metric_(self, topic, shardscope):
        extent_datapage = self.extent_datapage(topic, **shardscope)
        if self.debug:
            print(f"_extent_shard_metric_: topic: {repr(topic)}: shardspace: {shardscope}: extent_datapage: {extent_datapage}")
        if len(extent_datapage) > 1:
            raise ValueError(f"Too many shards in extent_datapage: {extent_datapage}")
        if len(extent_datapage) == 0:
            metric = 0
        else:
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
                shard_metric = self._extent_shard_metric_(topic, blockscope)
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
        #TODO: ensure topic=None is handled correctly: `topic == None` must only be allowed when `self.topics == None` 
        #TODO: disallow the current default `self.topics == [None]` -> `self.topic = None`
        #TODO: when `self.topics is not None` it must be a list of valid str
        #TODO: `self.topics == None` must mean that `_shardspace_` generates a unique space corresponding to an `hvhandle` with no topic head
        #TODO: `scope_to_hvhandle` with topic==None must generate an hvhandle with no topic head
        tagshard = self._tagscope_(**shard)
        hvhandle = self._scope_to_hvhandle_(topic, **tagshard)
        subspace = self.versionspace.subspace(*hvhandle)
        if self.debug:
            print(f"SHARDSPACE: formed for topic {repr(topic)} and shard with tag {tagshard}: {subspace}")
        return subspace

    def _scope_to_hvhandle_(self, topic, **shard):
        if topic is not None:
            shardhivechain = [topic]
        else:
            shardhivechain = []
        for key in self.shard_keys:
            shardhivechain.append(f"{key}={shard[key]}")
        return shardhivechain

    def _scope_to_kvhandle_(self, topic, **scope):
        _kvhandle = tuple((key, scope[key]) for key in self.shard_keys)
        kvhandle = KVHandle(_kvhandle)
        return kvhandle

    def _kvhandle_to_scope_(self, topic, kvhandle):
        return {key: val for key, val in kvhandle}

    def _kvhandle_to_hvhandle_(self, topic, kvhandle):
        scope = self._kvhandle_to_scope_(topic, kvhandle)
        hivechain = self._scope_to_hvhandle_(topic, **scope)
        return hivechain

    def _kvhandle_to_filename(self, topic, kvhandle, extension=None):
        datachain = self._kvhandle_to_hvhandle_(topic, kvhandle)
        name = '.'.join(datachain)
        filename = name if extension is None else name+'.'+extension
        return filename

    def _kvhandle_to_dirpath(self, topic, kvhandle):
        datachain = self._kvhandle_to_hvhandle_(topic, kvhandle)
        subspace = self.versionspace.subspace(*datachain)
        path = subspace.root
        return path
    
    def _databook_kvhandle_to_scope(self, databook):
        _databook = {}
        for topic, datapage in databook.items():
            if not topic in _databook:
                _databook[topic] = ()
            for kvhandle, pathset in datapage.items():
                scope = self._kvhandle_to_scope_(topic, kvhandle)
                _databook[topic] += (scope, pathset)
        return _databook

    def _lock_kvhandle(self, topic, kvhandle):
        if self.lock_pages:
            hivechain = self._kvhandle_to_hvhandle_(topic, kvhandle)
            self.versionspace.subspace(*hivechain).acquire()

    def _unlock_kvhandle(self, topic, kvhandle):
        if self.lock_pages:
            hivechain = self._kvhandle_to_hvhandle_(topic, kvhandle)
            self.versionspace.subspace(*hivechain).release()

    def _recordspace_(self):
        subspace = self.anchorspace.subspace('.records', f'schema={self.RECORD_SCHEMA_VERSION}')
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

    def show_build_records(self, *, full=False, columns=None, all=False, tail=5):
        """
        All build records for a given Databuilder class, irrespective of alias and version (see `list()` for more specific).
        'all=True' forces 'tail=None'
        """
        short = not full
        recordspace = self._recordspace_()

        frame = None
        try:
            #DEBUG
            #print(f">>> recordspace: {recordspace}")
            #DEBUG
            filepaths = [recordspace.join(recordspace.root, filename) for filename in recordspace.list()]
            frames = [pd.read_parquet(filepath, storage_options=recordspace.filesystem.storage_options) for filepath in filepaths]
            #DEBUG
            #for i, frame in enumerate(frames):
                #print(f"frame: {i} {frame}")

            frame = pd.concat(frames) if len(frames) > 0 else pd.DataFrame()

            #DEBUG
            #print(f"_frame: {_frame}")

            #parquet_dataset = pq.ParquetDataset(recordspace.root, use_legacy_dataset=False, filesystem=recordspace.filesystem)
            #table = parquet_dataset.read()
            #frame = table.to_pandas()
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
                _columns_ = [c for c in _columns if c in Databuilder.BUILD_RECORDS_COLUMNS_SHORT]
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

    def show_named_record(self, *, alias=None, version=None, full=False):
        import datablocks # needed for `eval`
        records = self.show_build_records(full=full)
        if self.debug:
            print(f"show_name_record: databuilder {self}: version: {repr(version)}, alias: {repr(alias)}: retrieved records: len: {len(records)}:\n{records}")
        if len(records) == 0:
            return None
        if alias is not None:
            records0 = records.loc[records.alias == alias]
        else:
            records0 = records
        if version is not None:
            records1 = records0.loc[records0.version == repr(version)] #NB: must compare to string repr
        else:
            records1 = records0
        if self.debug:
            print(f"show_name_record: filtered records1: len: {len(records1)}:\n{records1}")
        if len(records1) > 0:
            record = records1.iloc[-1]
        else:
            record = None
        return record

    def show_build_graph(self, *, record=None, node=tuple(), show=('logpath', 'logpath_status', 'exception'), _show=tuple(), **kwargs):
        _record = self.show_build_record(record=record, full=True)
        if _record is None:
            return None
        _transcript = _record['report_transcript']
        if _transcript is None:
            return None
        if isinstance(show, str):
            show=(show,)
        if isinstance(_show, str):
            _show=(_show,)
        show = show + _show
        _graph = Graph(_transcript, show=show, **kwargs)
        if node is not None:
            graph = _graph.node(*node)
        else:
            graph = _graph
        return graph
    
    def show_build_batch_count(self, *, record=None):
        g = self.show_build_graph(record=record) 
        nbatches = len(g.args)-1 # number of arguments less one to the outermost Request AND(batch_request[, batch_request, ...], extent_request)
        return nbatches
    
    def show_build_batch_graph(self, *, record=None, batch=0, **kwargs):
        g = self.show_build_graph(record=record, node=(batch,), **kwargs) # argument number `batch` to the outermost Request AND
        return g
    
    def show_build_transcript(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        summary = _record['report_transcript']
        return summary

    def show_build_scope(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        scopestr = _record['scope']
        scope = _eval(scopestr)
        return scope

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
            # TODO: report_transcript should be just a repr(report.to_dict()), ideally, repr(report)
            # TODO: however, we may rewrite report.result
            task = request.task
            timestamp = int(microseconds_since_epoch())
            datestr = datetime_to_microsecond_str()
            blockscopestr = repr(blockscope)
            _record = dict(schema=Databuilder.RECORD_SCHEMA_VERSION,
                           alias=alias,
                           stage=lifecycle_stage.name,
                           classname=classname,
                            version=versionstr,
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
                            metric='',
                            report_transcript='',
            )
        
            if self.verbose:
                    print(f"DATABOOK LIFECYCLE: {lifecycle_stage.name}: writing record for request:\n{request}")
                    print(f"DATABOOK LIFECYCLE: {lifecycle_stage.name}: topics: {self.topics}")
                    print(f"DATABOOK LIFECYCLE: {lifecycle_stage.name}: blockscope: {blockscope}")
            
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
                    print(f"DATABOOK LIFECYCLE: {lifecycle_stage.name}: transcript: {transcriptstr}, logpath: {logpath}, logname: {logname}")
                _record.update(dict(
                                    task_id=str(task_id),
                                    runtime_secs=f"{runtime_secs}",
                                    status=report_transcript['status'],
                                    success=report_transcript['success'],
                                    logspace=repr(logspace),
                                    logname=logname,
                                    report_transcript=transcriptstr,
                            ))
            records = []
            shard_list = self.scope_to_shards(**blockscope)
            for shard in shard_list:
                for topic in self.topics:
                    record = copy.deepcopy(_record)
                    if response is not None:
                        metric = self._extent_shard_metric_(topic, shard)
                        record['metric'] = str(metric)
                    record['topic'] = topic
                    record['shardscope'] = repr(shard)
                    records.append(record)
            record_frame = pd.DataFrame.from_records(records)
            record_frame.index.name = 'index'
            record_filepath = \
                recordspace.join(recordspace.root, f"alias-{alias}-stage-{lifecycle_stage.name}-task_id-{task_id}-datetime-{datestr}.parquet")
            if self.verbose:
                print(f"DATABOOK LIFECYCLE: {lifecycle_stage.name}: Writing build record at lifecycle_stage {lifecycle_stage.name} to {record_filepath}")
            record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)
        return _write_record_lifecycle_callback

    def build_request(self, **scope):
        request = Request(self.build, **scope).apply(self.pool)
        return request

    def build(self, **scope):
        import datablocks
        def scopes_equal(s1, s2):
            if set(s1.keys()) != (s2.keys()):
                return False
            for key in s1.keys():
                if s1[key] != s2[key]:
                    return False
            return True
        # TODO: consistency check: sha256 alias must be unique for a given version or match scope
        blockscope = self._blockscope_(**scope)
        record = self.show_named_record(alias=self.alias, version=self.version) 
        if record is not None:
            _scope = eval(record['scope'])
            if _scope is not None and not scopes_equal(blockscope, _scope):
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
    def provide_databook_request(self, **scope):
        blockscope = self._blockscope_(**scope)
        if self.reload:
            shortfall_databook = self.intent_databook(**blockscope)
        else:
            shortfall_databook = self.shortfall_databook(**blockscope)
        shortfall_databook_kvhandles_lists = [list(shortfall_databook[topic].keys()) for topic in self.topics]
        shortfall_databook_kvhandles_list = [_ for __ in shortfall_databook_kvhandles_lists for _ in __]
        shortfall_databook_kvhandles = list(set(shortfall_databook_kvhandles_list))

        shortfall_batchscope_list = \
            self._kvhandles_to_batches_(*shortfall_databook_kvhandles)
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
            self._kvhandles_to_batches_(*shortfall_databook_kvhandles)
        shortfall_batchscope_list = \
            [{k: blockscope[k] for k in tscope.keys()} for tscope in shortfall_batchscope_list]
        if self.verbose:
            if len(shortfall_batchscope_list) == 0:
                print(f"build_databook_request: no shortfalls found: returning extent")
            else:
                print(f"build_databook_request: requesting build of shortfall batchscopes with tags: {shortfall_batchscope_list}")
        _shortfall_batch_requests = \
            [self._build_batch_request_(self._tagscope_(**shortfall_batchscope_list[i]), **shortfall_batchscope_list[i])
                            .apply(self.pool) for i in range(len(shortfall_batchscope_list))]
        shortfall_batch_requests = [_.apply(self.pool) for _ in _shortfall_batch_requests]
        shortfall_batch_requests_repr= "[" + \
                                          ", ".join(repr(_) for _ in shortfall_batch_requests) + \
                                          "]"
        if self.verbose:
            print(f"build_databook_request: shortfall_batch_requests: " + shortfall_batch_requests_repr)

        tagscope = self._tagscope_(**blockscope)
        extent_request = Request(self.extent, **tagscope)
        requests = shortfall_batch_requests + [extent_request]
        build_databook_request = LAST(*requests)
        #TODO: #FIX
        #build_databook_request = LAST(*shortfall_batch_requests) if len(shortfall_batch_requests) > 0 else NONE()
        return build_databook_request

    @OVERRIDE
    # tagscope is necessary since batchscope will be expanded before being passed to _build_batch_
    def _build_batch_request_(self, tagscope, **batchscope):
        self.versionspace.ensure()
        return Request(self._build_batch_, tagscope, **batchscope)

    @OVERRIDE
    def _build_batch_(self, tagscope, **batchscope):
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




class DBX:
    """
        DBX instantiates a Databuilder class on demand, in particular, different instances depending on the build(**kwargs),
            . pool=Ray()
            . etc
        Not inheriting from Databuilder also has the advantage of hiding the varable scope API that Databuilder.build/read/etc presents.
        DBX is by definition a fixed scope block.
    """

    class Reader(request.Proxy):
        def __init__(self, *, locator):
            """
                locator: str | tuple[DBX, topic:str]
            """
            if isinstance(locator, dict):
                self.dbx, self.topic = locator['dbx'], locator['topic']
            elif isinstance(locator, str):
                self.dbx, self.topic = DBX.parse_locator(locator)

        @property
        def request(self):
            _ = self.dbx.read_request(topic=self.topic)
            return _
        
        def __ne__(self, other):
            return not isinstance(other, self.__class__) or \
                self.request != other.request
        
        def __eq__(self, other):
            return not self.__ne__(other)
 
        def __tag__(self):
            tag__ = f"{DBX_PREFIX}.{self.dbx.datablock_clstr}"
            tag_ = tag__+f"@{self.dbx.alias}" if self.dbx.alias is not None else tag__
            tag = tag_ + f":{self.topic}" if self.topic != DEFAULT_TOPIC else tag_
            return tag
        
        def __str__(self):
            #TODO: .repr_ctor(..., locator=self.__tag__())
            _ = self.__repr__()
            return _ 
        def __repr__(self):
            _ = Tagger(tag_defaults=False).repr_ctor(self.__class__, locator=dict(dbx=self.dbx, topic=self.topic))
            return _
    
    @staticmethod
    def parse_locator(locator: str, **datablock_kwargs):
        parts = locator.split(":")
        datablock_clstr, alias = head.split('@')
        dbx = DBX(datablock_clstr, alias, **datablock_kwargs)
        return dbx, topic

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
                *,
                debug=False,
                verbose=False,):
        self.alias = alias
        self.debug = debug
        self.verbose = verbose

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

        self.datablock_kwargs = {}
        @functools.wraps(self.datablock_cls)
        def update_datablock_kwargs(**datablock_kwargs):
            self.datablock_kwargs.update(**datablock_kwargs)
            return self
        self.Datablock = update_datablock_kwargs

        self._scope = None # initialize to default scope
        @functools.wraps(self.datablock_cls.SCOPE)
        def update_scope(**scope):
            if self._scope is None:
                self._scope = copy.copy(scope)
            else:
                self._scope.update(**scope)
            return self
        self.SCOPE = update_scope
        # scope is extracted via a property, which validates the scope

        self.databuilder_kwargs = dict(
            dataspace=DATABLOCKS_DATALAKE,
            pool=DATABLOCKS_FILE_LOGGING_POOL,
            tmpspace=None, # derive from dataspace?
            lock_pages=False,
            throw=True, # fold into `pool`?
            rebuild=False, # move to build()?
            verbose=False,
            debug=False,
        )
        @functools.wraps(Databuilder)
        def update_databuilder_kwargs(**kwargs):
            self.databuilder_kwargs.update(**kwargs)
            return self
        self.Databuilder = update_databuilder_kwargs

    def __repr__(self):
        """
        if self.alias is not None:
            _ =  Tagger().repr_func(self.__class__, self.datablock_clstr, self.alias)
        else:
            _ = Tagger().repr_func(self.__class__, self.datablock_clstr)
        """
        #FIX: do not make default self.alias None explicit
        _ =  Tagger().repr_func(self.__class__, self.datablock_clstr, self.alias, debug=self.debug, verbose=self.verbose)
        return _

    # TODO: spell out `.DATABOOK()` and `.SCOPE()` modifications?
    def __tag__(self):
        _tag = f"{DBX_PREFIX}.{self.datablock_clstr}"
        if self.alias is not None:
            _tag += f"@{self.alias}"
        tag = f"'{_tag}'"
        return tag

    def __hash__(self):
        _repr = self.__tag__()
        _ =  int(hashlib.sha1(_repr.encode()).hexdigest(), 16)
        return _
    
    def __getattr__(self, attr):
        return getattr(self.databuilder, attr)

    @property
    def databuilder(self):
        databuilder_cls = self._make_databuilder_class()
        databuilder = databuilder_cls(self.alias, **self.databuilder_kwargs)
        return databuilder

    @property
    def scope(self):
        if self._scope is not None:
            _scope = self._scope
            if self.verbose:
                print(f"DBX: scope: databuilder {self.databuilder} with version {self.databuilder.version} with alias {repr(self.databuilder.alias)}: using specified scope: {self._scope}")
        else:
            record = self.databuilder.show_named_record(alias=self.databuilder.alias, version=self.databuilder.version) 
            if record is not None:
                _scope = eval(record['scope'])
                if self.verbose:
                    print(f"DBX: scope: no specified scope for databuilder {self.databuilder} with version {self.databuilder.version} with alias {repr(self.databuilder.alias)}: using build record scope: {_scope}")
            else:
                _scope = {}
                if self.verbose:
                    print(f"DBX: scope: no specified scope and no records for databuilder {self.databuilder} with version {self.databuilder.version} with alias {repr(self.databuilder.alias)}: using default scope")
        return _scope

    def build_request(self):
        build_request = self.databuilder.build_request(**self.scope)
        return build_request

    def build(self):
        result = self.databuilder.build(**self.scope)
        return result

    def read_request(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"{self} of version {self.version} has not been built yet")
        request = self.databuilder.read_databook_request(topic, **self.scope)\
            .set(summary=lambda _: self.extent()[topic])
        return request
    
    def data(self, topic=None):
        reader = self.reader(topic)
        return reader

    def reader(self, topic=DEFAULT_TOPIC):
        reader = DBX.Reader(locator=dict(dbx=self, topic=topic))
        return reader
    
    def read(self, topic=DEFAULT_TOPIC):
        read_request = self.read_request(topic)
        result = read_request.compute()
        return result
    
    def intent(self):
        _ = self.databuilder.intent(**self.scope)
        return _

    def extent(self):
        _ = self.databuilder.extent(**self.scope)
        return _
    
    def extent_metric(self):
        _ = self.databuilder.extent_metric(**self.scope)
        return _    
    
    def valid(self):
        _ = self.extent()
        return _

    def metric(self, topic=None):
        _ = self.extent_metric()
        return _
    
    def _validate_subscope(self, **subscope):
        #TODO: check that subscope is a subscope of self.scope
        return subscope
    
    def _make_databuilder_class(dbx):
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

        @property
        def __datablock(self):
            if self._datablock is None:
                self._datablock = self.datablock_cls(**self.datablock_kwargs)
            return self._datablock

        def __datablock_shardroots(self, tagscope, ensure=False) -> Union[str, Dict[str, str]]:
            shardscope_list = self.scope_to_shards(**tagscope)
            assert len(shardscope_list) == 1
            shardscope = shardscope_list[0]
            if hasattr(self.datablock_cls, 'TOPICS'):
                shardroots = {}
                for _topic in self.datablock_cls.TOPICS:
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

        def __datablock_blockroots(self, tagscope, ensure=False) -> Union[Union[str, List[str]], Dict[str, Union[str, List[str]]]]:
            #TODO: implement blocking: return a dict from topic to str|List[str] according to whether this is a shard or a block
            if len(self.block_to_shard_keys) > 0:
                raise NotImplementedError(f"Batching not supported at the moment: topic: tagscope: {tagscope}")
            blockroots = self.datablock_shardroots(tagscope, ensure=ensure)
            return blockroots

        def __datablock_batchroots(self, tagscope, ensure=False) -> Union[Union[str, List[str]], Dict[str, Union[str, List[str]]]]:
            #TODO: implement batching: return a dict from topic to str|List[str] according to whether this is a shard or a batch
            if len(self.batch_to_shard_keys) > 0:
                raise NotImplementedError(f"Batching not supported at the moment: topic: tagscope: {tagscope}")
            batchroots = self.datablock_shardroots(tagscope, ensure=ensure)
            return batchroots

        def __build_batch__(self, tagscope, **batchscope):
            datablock_batchscope = self.datablock_cls.SCOPE(**batchscope)
            datablock_shardroots = self.datablock_batchroots(tagscope, ensure=True)
            #DEBUG
            #print(f"__build_batch__: datablock_shardroots: {datablock_shardroots}")
            self.datablock.build(datablock_shardroots, scope=datablock_batchscope, filesystem=self.dataspace.filesystem)
            _ = self.extent_databook(**tagscope)
            return _

        def __read_block__(self, tagscope, topic, **blockscope):
            # tagscope can be a list, opaque to the Request evaluation mechanism, but batchscope must be **-expanded to allow Request mechanism to evaluate the kwargs
            datablock_blockscope = self.datablock_cls.SCOPE(**blockscope)
            datablock_blockroots = self.datablock_blockroots(tagscope)
            if topic == None:
                assert not hasattr(self.datablock, 'TOPICS'), f"__read_block__: None topic when datablock.TOPICS == {self.datablock.TOPICS} "
                _ = self.datablock.read(datablock_blockroots, scope=datablock_blockscope, filesystem=self.dataspace.filesystem)
            else:
                _ = self.datablock.read(datablock_blockroots, topic=topic, scope=datablock_blockscope, filesystem=self.dataspace.filesystem)
            return _

        def __extent_shard_valid__(self, topic, tagshardscope):
            datablock_tagshardscope = self.datablock_cls.SCOPE(**tagshardscope)
            datablock_shardroots = self.datablock_shardroots(topic, tagshardscope)
            if topic == None:
                assert not hasattr(self.datablock, 'TOPICS'), f"__extent_shard_valid__: None topic when datablock.TOPICS == {getattr(self.datablock, 'TOPICS')} "
                _ = self.datablock.valid(scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem, roots=datablock_shardroots)
            else:
                _ = self.datablock.valid(scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem, roots=datablock_shardroots, topic=topic)
            return _
        
        def __extent_shard_metric__(self, topic, tagshardscope):
            datablock_tagshardscope = self.datablock_cls.SCOPE(**tagshardscope)
            datablock_shardroots = self.datablock_shardroots(topic, tagshardscope)
            if topic == None:
                assert hasattr(self.datablock, 'TOPICS'), f"__extent_shard_metric__: None topic when datablock.TOPICS == {self.datablock.TOPICS} "
                _ = self.datablock.metric(scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem, roots=datablock_shardroots)
            else:
                _ = self.datablock.metric(scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem, roots=datablock_shardroots, topic=topic)
            return _

        SCOPE_fields = dataclasses.fields(dbx.datablock_cls.SCOPE)
        __block_keys = [field.name for field in SCOPE_fields]
        __block_defaults = {field.name: field.default  for field in SCOPE_fields if field.default != dataclasses.MISSING}
        __batch_to_shard_keys = {field.name: field.name for field in SCOPE_fields if isinstance(field.type, RANGE)}

        '''
        try:
            importlib.import_module(databuilder_module_name)
        except:
            spec = importlib.machinery.ModuleSpec(databuilder_module_name, None)
            mod = importlib.util.module_from_spec(spec)
        '''

        __module_name = DBX_PREFIX + "." + dbx.datablock_module_name
        __cls_name = dbx.datablock_cls.__name__
        __datablock_cls = dbx.datablock_cls
        __datablock_kwargs = dbx.datablock_kwargs
        if hasattr(dbx.datablock_cls, 'TOPICS'):
            __topics = dbx.datablock_cls.TOPICS
        else:
            __topics = [DEFAULT_TOPIC]

        #DEBUG
        #print(f"------> __topics: {__topics}")
        if hasattr(dbx.datablock_cls, 'VERSION'):
            __version = dbx.datablock_cls.VERSION
        else:
            __version = DEFAULT_VERSION
        databuilder_classdict = {
                    '__module__': __module_name,
                    'block_keys': __block_keys, 
                    'block_defaults': __block_defaults,
                    'batch_to_shard_keys': __batch_to_shard_keys,
                    '__init__': __init__,
                    '__repr__': __repr__,
                    'version': __version,
                    'topics': __topics,
                    'datablock_cls': __datablock_cls,
                    'datablock_kwargs': __datablock_kwargs,
                    'datablock': __datablock,
                    'datablock_blockroots': __datablock_blockroots,
                    'datablock_batchroots': __datablock_batchroots,
                    'datablock_shardroots': __datablock_shardroots,
                    '_build_batch_': __build_batch__,
                    '_read_block_':  __read_block__,
        }
        if hasattr(dbx.datablock_cls, 'valid'):
            databuilder_classdict['_extent_shard_valid_'] = __extent_shard_valid__
        if hasattr(dbx.datablock_cls, 'metric'):
            databuilder_classdict['_extent_shard_metric_'] = __extent_shard_metric__

        databuilder_class = type(__cls_name, 
                               (Databuilder,), 
                               databuilder_classdict,)
        return databuilder_class

    @property
    def tagscope(self):
        _ = self.databuilder._tagscope_(**arg.dbx.scope)
        return _

    @staticmethod
    def transcribe(*dbxs, verbose=False, with_home=False, with_linenos=False, with_display=False):
        """
            Assume dbxs are ordered in the dependency order and all have unique aliases that can be used as variable prefixes.
            TODO: build the dependency graph and reorder, if necessary.
        """

        script = ""
        imports = ""
        if with_home:
            imports += "import os\n"
        imports += "import fsspec\n"

        #TODO: collect and preload all arg.dbxs
        
        if with_home:
            script += "HOME = os.getenv('HOME')\n\n"
        for dbx in dbxs:
            imports += f"import {dbx.datablock_module_name}\n"

            _datablock = Tagger().tag_ctor(dbx.datablock_cls, **dbx.datablock_kwargs)
            script += f"# {tag(dbx)}\n"
            blockscope = dbx.databuilder._blockscope_(**dbx.scope)
            tagscope = dbx.databuilder._tagscope_(**blockscope)
            _blockscope_name = f"{dbx.alias}_scope"
            _blockscope_val  = f"{dbx.datablock_clstr}.SCOPE(\n"
            for key, arg in dbx.scope.items():
                if isinstance(arg, DBX.Reader):
                    _argdatablock = Tagger().tag_ctor(arg.dbx.datablock_cls, **arg.dbx.datablock_kwargs)
                    _argblockscope = arg.dbx.scope
                    _argtagscope = arg.dbx.databuilder._tagscope_(**_argblockscope)
                    _argprotocol = arg.dbx.databuilder.dataspace.protocol
                    _argprotocol = _argprotocol if _argprotocol else "file"
                    _argstorage_options = arg.dbx.databuilder.dataspace.script_storage_options()
                    _argfilesystem = signature.Tagger().tag_func("fsspec.filesystem", _argprotocol, **_argstorage_options)
                    _blockscope_val += f"\t{key}={_argdatablock}.read(\n" + \
                                       f"{repr(arg.topic)}),\n" + \
                                       f"{_argtagscope}),\n" + \
                                       f"{_argfilesystem}),\n" + \
                                       f"{repr(_argtagscope)},\n" + \
                                       f")\n"
                else:
                    _blockscope_val += f"\t{key}={repr(arg)},\n"
            _blockscope_val += ")\n"
            
            blockroots = dbx.databuilder.datablock_blockroots(tagscope)
            _filesystem_name = f"{dbx.alias}_filesystem"
            _protocol = dbx.databuilder.dataspace.protocol
            _protocol = _protocol if _protocol else "file"
            _storage_options = dbx.databuilder.dataspace.script_storage_options()
            _filesystem_val = signature.Tagger().tag_func("fsspec.filesystem", _protocol, **_storage_options)
            script += f"{_filesystem_name} = {_filesystem_val}\n"
            if isinstance(blockroots, str):
                blockroots_list = [blockroots]
            if isinstance(blockroots, dict):
                blockroots_list = list(blockroots.values())
            if isinstance(blockroots, list):
                blockroots_list = blockroots
            for blockroot in blockroots_list:
                    script += f"{_filesystem_name}.mkdirs({repr(blockroot)}, exist_ok=True)\n"
            _blockroots = repr(blockroots)

            script += f"\n{_blockscope_name} = {_blockscope_val}"
            script += f"\n{_datablock}.build(\n"  +\
                      f"\t{_blockscope_name},\n" +\
                      f"\t{_filesystem_name},\n" +\
                      f"\t{_blockroots}\n"  +\
                      f")\n"
            script += "\n"

            s = imports + "\n\n\n" + script
            if with_linenos: 
                lines = s.split('\n')
                #width = round(int(math.log(len(lines))))
                ss = '\n'.join([f"{i:>4}: {ln}" for i, ln in enumerate(lines)]) #TODO: use width for padding
            else:
                ss = s                 
        return ss
            
            

            
    
    