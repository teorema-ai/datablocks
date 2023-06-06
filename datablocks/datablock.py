import collections
import copy
import datetime
import functools
import hashlib
import importlib
import logging
import os
import tempfile
import types

import pdb

import yaml

import fsspec
import pyarrow.parquet as pq
import pandas as pd

from . import config, utils
from . import signature as tag
import datablocks
from .signature import Signature, func_kwonly_parameters, func_kwdefaults, ctor_name
from .signature import Tagger
from .utils import DEPRECATED, OVERRIDE, REMOVE, ALIAS, EXTRA, RENAME, BROKEN, BOOL, microseconds_since_epoch, datetime_to_microsecond_str, docstr
from .eval.request import Request, Report, FIRST, LAST, Graph
from .eval.pool import DATABLOCKS_LOGGING_POOL, DATABLOCKS_LOGGING_REDIRECT_POOL
from .dataspace import Dataspace, DATABLOCKS_DATALAKE


_eval = __builtins__['eval']
_print = __builtins__['print']


logger = logging.getLogger(__name__)


HOME = os.environ['HOME']


#TODO: DATABLOCKS_POOL should be fed into a Pool factory and trigger the reading of DATABLOCKS_POOL_* environ by the factory.


"""
IDEA: 
* Datablock manages computation and storage of collections of data identified by kwarg `shards` using:
* COLLATING:
  - related 'topics' may be computed together, rather than independently, hence, nonparallelizable
* PARALLELIZING/BATCHING:
  - blockscope (or multi-kwargs: kwargs with plural keys and iterable values) are mapped onto concurrent or batched computations accordingly
  - concurrent computations are requested using a supplied pool
  - batch (or batch-kwargs)
* CACHING: 
  - existing datasets are not recomputed (caching)
  - and if they are recomputed, computations are minimized within the batching constraints
  
NOMENCLATURE:
* kvhandle:     ((key, val), ...,(key, val))               
* kvpath:       f"{key}={val}/{key}={val}/.../{key}={val}" # REMOVE
* kvhandlepath: (kvhandle, kvpath)                         # TODO: --> kvrecord: (kvhandle, filepathset)
* filepathset  dirpath | [filepath in filepathlist]

* datapage:    {kvhandle: filepathset, ...}                
* databook:    {topic: datapage, ...}                      # TODO: --> kvpathbook
* datachain:   (topic, f"{key}={val}", f"{key}={val}", ..., f"{key}={val}") # REMOVE
"""


class Anchored:
    def __init__(self, namechain=None):
        self.namechain = namechain

    @property
    def anchorchain(self):
        if not hasattr(self, 'anchor'):
            modchain = tuple(str(self.__class__.__module__).split('.'))
            anchorclassname = self.__class__.__qualname__.split('.')[0]
            anchorclasschain = modchain + (anchorclassname,)
        else:
            anchorclasschain = self.anchor
        if self.namechain:
            anchorchain = anchorclasschain + self.namechain
        else:
            anchorchain = anchorclasschain
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
                _val = [val]
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



DEFAULT_TOPIC = None
DEFAULT_VERSION = '0.0.1'
class Datablock(Anchored, Scoped):
    """
    API:
    * intent(topics=cls.topics, **blockscope)   # TODO: remove topics; use cls.topics
    * extent(topics=cls.topics, **blockscope)    # TODO: remove topics; use cls.topics
    * shortfall(topics=cls.topics, **blockscope) # TODO: remove topics; use cls.topics
    * update_request(topics=cls.topics, **blockscope)# TODO: remove topics; use cls.topics
    * clear_request(topics=cls.topics, **blockscope)# TODO: remove topics; use cls.topics

    FRAMEWORK:
    * Overridable methods form a hierarchy:
      - class forms a *framework*
      - innermost methods are overriden first
    * Generally 'databook' methods should be overriden first
      - and 'datapage' compute method should be factored through 'databook' methods
      - because of the dependencies between topics' generation
      - such as the parsing of 'depths', 'trades', 'matches' and 'orders' from a single PCAP
      - in a single process
    * Generally '_request' methods should be overriden last 
      - to reuse default eval request graph:
      - 'x_request' implements a request graph
      - in terms 'x' and 'pool'
      
    IMPLEMENT
    * TL;DR: implement these:
      - '_build_batch_(tagbatchscope, **batchscope)':   
        . write all pages (whole book) for all shards
        . use scope_to_shards to break up the batchscope
        . use [default] '_shardspace_().root' for path(s) to write to
      - ['_shardspace_()']: 
        . used for reading

    OVERRIDE:
    * In case simple impl is insufficient
    * Override priority ranking
      - '_build_batch_(tagbatchscope, **batchscope)'         
      - '_build_batch_request__(tagbatchscope, **batchscope)'

      - '_shardspace_(dataspace, topic, **shardscope)': # dataspace should be self.versionspace unless self.tmpspace is being used.
     
      - 'build(**blockscope)'
      - 'build_request(**blockscope)'
      - 'UNSAFE_clear_request(**blockscope)'
    FIX:
    * Reconcile verbose and logging?  
    * Use logger/logging in Datablock/Datastack, but verbose in the user impl.
    """
    # TODO: implement support for multiple batch_to_shard_keys
    record_schema = '0.0.1'
    topics = []
    signature = Signature((), ('dataspace', 'version',)) # extract these attrs and use in __tag__

    # TODO: make dataspace, version position-only and adjust Signature
    def __init__(self,
                 alias=None,
                 *,
                 dataspace=DATABLOCKS_DATALAKE,
                 tmpspace=None,
                 version=DEFAULT_VERSION,
                 lock_pages=False,
                 throw=True,
                 rebuild=False,
                 verbose=False,
                 build_echo_task_id=True,
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
        self.pool = pool
        self.reload = rebuild
        self.throw = throw
        self.build_echo_task_id = build_echo_task_id
        if self.lock_pages:
            raise NotImplementedError("self.lock_pages not implemented for Datablock")
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

    def get_topics(self, print=False):
        if print:
            __build_class__['print'](self.topics)
        return self.topics

    def get_version(self, print=False):
        if print:
            __build_class__['print'](self.topics)
        return self.version

    def intent_datapage(self, topic, **scope):
        blockscope = self._blockscope_(**scope)
        tagblockscope = self._tagscope_(**blockscope)
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        shard_list = self.scope_to_shards(**tagblockscope)
        kvhandle_pathshard_list = []
        for shard in shard_list:
            kvhandle = self._scope_to_kvchain_(topic, **shard)
            pathshard = self._shardspace_(self.versionspace, topic, **shard).root
            kvhandle_pathshard_list.append((kvhandle, pathshard))
        intent_datapage = {kvhandle: pathshard for kvhandle, pathshard in kvhandle_pathshard_list}
        return intent_datapage

    @OVERRIDE
    def _extent_shard_valid_(self, pathset, topic, **shardscope):
        valid = False
        if isinstance(pathset, str):
            if self.versionspace.filesystem.isdir(pathset):
                valid = True
        else:
            _pathset = [path for path in pathset if self.versionspace.filesystem.isfile(path)]
            valid = (len(_pathset) == len(pathset))
        return valid

    def extent_datapage(self, topic, **scope):
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        intent_datapage = self.intent_datapage(topic, **scope)
        extent_datapage = {}
        for kvhandle, shard_pathset in intent_datapage.items():
            shardscope = self._kvchain_to_scope_(topic, kvhandle)
            valid = self._extent_shard_valid_(shard_pathset, topic, **shardscope)
            if valid:
                extent_datapage[kvhandle] = shard_pathset
        return extent_datapage

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

    def intent_databook(self, **scope):
        return self._page_databook("intent", **scope)

    def extent_databook(self, **scope):
        return self._page_databook("extent", **scope)

    @staticmethod
    def collate_datapages(topic, *datapages):
        # Each datapage is a dict {kvhandle -> filepathset}.
        # `collate` is just a union of dicts.
        # Assuming each kvhandle exists only once in datapages or only the last occurrence matters.
        collated_datapage = {kvhandle: filepathset for datapage in datapages for kvhandle, filepathset in datapage.items()}
        return collated_datapage

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
            {topic: Datablock.collate_datapages(topic, *datapages) \
             for topic, datapages in topic_datapages.items()}
        return collated_databook

    def intent(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        intent_databook = self.intent_databook(**blockscope)
        _intent_databook = self._databook_kvchain_to_scope(intent_databook)
        return _intent_databook

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
    
    # TODO: factor into extent_datapage_metric, extent_databook_metric and extent_metric?
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

    def shortfall_databook(self, **scope):
        blockscope = self._blockscope_(**scope)
        pagekvhandles_list = [set(self.shortfall_datapage(topic, **blockscope).keys()) for topic in self.topics]
        bookkvhandleset = set().union(*pagekvhandles_list)
        shortfall_databook = {}
        for topic in self.topics:
            shortfall_datapage = {}
            for kvhandle in bookkvhandleset:
                scope = {key: val for key, val in kvhandle}
                filepathset = self._shardspace_(self.versionspace, topic, **blockscope).root
                shortfall_datapage[kvhandle] = filepathset
            shortfall_databook[topic] = shortfall_datapage
        return shortfall_databook
    

    def _shard_pathset_(self, dataspace, topic, **shard):
        shardspace = self._shardspace(dataspace, topic, **shard)
        dirpath = shardspace.root
        return dirpath
    

    @OVERRIDE
    def _shardspace_(self, dataspace, topic, **shard):
        tagshard = self._tagscope_(**shard)
        hvchain = self._scope_to_hvchain_(topic, **tagshard)
        subspace = dataspace.subspace(*hvchain).ensure()
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
        kvhandle = tuple((key, scope[key]) for key in self.shard_keys)
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

    def _page_databook(self, domain, **kwargs):
        # This databook is computed by computing a page for each topic separately, 
        # via a dedicated function call with topic as an arg, using a domain-specific
        # method.  domain: 'intent'|'extent'|'shortfall'
        _datapage = getattr(self, f"{domain}_datapage")
        databook = {topic: _datapage(topic, **kwargs) for topic in self.topics}
        return databook

    def print_metric(self, **scope):
        metric = self.extent_metric(**scope)
        print(metric)
    
    BUILD_RECORDS_COLUMNS_SHORT = ['stage', 'version', 'scope', 'alias', 'task_id', 'metric', 'status', 'date', 'timestamp', 'runtime_secs']

    def show_block_records(self, *, print=False, full=False, columns=None, all=False, tail=5):
        """
        All build records for a given Datablock class, irrespective of alias and version (see `list()` for more specific).
        'all=True' forces 'tail=None'
        """
        short = not full
        recordspace = self._recordspace_()

        parquet_dataset = pq.ParquetDataset(recordspace.root, use_legacy_dataset=False, filesystem=recordspace.filesystem)
        table = parquet_dataset.read()
        frame = table.to_pandas()
    
        if len(frame) > 0:
            
            frame.reset_index(inplace=True, drop=True)
            if columns is None:
                _columns = frame.columns
            else:
                _columns = [c for c in columns if c in frame.columns]
            if short:
                _columns_ = [c for c in _columns if c in Datablock.BUILD_RECORDS_COLUMNS_SHORT]
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

    def show_block_record_columns(self, *, full=True, **ignored):
        frame = self.show_block_records(full=full)
        columns = frame.columns
        return columns
    
    def show_block_record(self, *, record=None, full=False):
        import datablocks
        try:
            records = self.show_block_records(full=full)
        except:
            return None
        if len(records) == 0:
            return None
        if isinstance(record, int):
            _record = records.loc[record]
        if record is None:
            _record = records.iloc[-1]
        return _record

    def show_block_graph(self, *, record=None, node=tuple(), print=('logpath', 'logpath_status', 'exception'), _print=tuple(), **kwargs):
        _record = self.show_block_record(record=record, full=True)
        _summary = _record['report_summary']
        if isinstance(print, str):
            print=(print,)
        if isinstance(_print, str):
            _print=(_print,)
        print = print + _print
        _graph = Graph(_summary, print=print, **kwargs)
        if node is not None:
            graph = _graph.node(*node)
        else:
            graph = _graph
        return graph
    
    def show_block_nbatches(self, *, record=None, print=True):
        g = self.show_block_graph(record=record, node=(1,)) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        nbatches = len(g.args)
        if print:
            _print(nbatches)
        else:
            return nbatches
    
    def show_block_batch_graph(self, *, record=None, batch=0, **kwargs):
        g = self.show_block_graph(record=record, node=(1,batch), **kwargs) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        return g

    def show_block_batch_log(self, *, record=None, batch=0, **kwargs):
        g = self.show_block_graph(record=record, node=(1,batch), **kwargs) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        log = g.log()
        return log
    
    def show_block_batch_logpath(self, *, record=None, batch=0):
        g = self.show_block_graph(record=record, node=(1,batch)) # inner `collate_databooks()`, aka 2nd arg to outer `collate_databooks` (top of the graph)
        logpath = g.logpath()
        return logpath
    
    def show_block_summary(self, *, record=None, **ignored):
        _record = self.show_block_record(record=record, full=True)
        summary = _record['report_summary']
        return summary

    def show_block_scope(self, *, record=None, **ignored):
        _record = self.show_block_record(record=record, full=True)
        scopestr = _record['scope']
        scope = _eval(scopestr)
        return scope

    def show_block_logspace(self, *, record=None, **ignored):
        _record = self.show_block_record(record=record, full=True)
        _logspace = _record['logspace']
        logspace = _eval(_logspace)
        return logspace
    
    def show_block_logname(self, *, record=None, **kwargs):
        record = self.show_block_record(record=record, full=True)
        _ = record['logname']
        return _
    
    def show_block_logpath(self, *, record=None, **kwargs):
        logspace = self.show_block_logspace(record=record)
        logname = self.show_block_logname(record=record)
        if logspace and logname:
            logpath = logspace.join(logspace.root, logname+'.log')
            return logpath

    @DEPRECATED # USE show_block_graph()
    def show_block_logs(self, *, record=None, request_max_len=50, **kwargs):
        g = self.show_block_graph(record=record, **kwargs)
        if g is not None:
            _ = g.validate_logs(request_max_len=request_max_len)
            return _
        
    @DEPRECATED # USE show_block_graph()
    def show_block_request(self, *, record=None, max_len=None, **kwargs):
        kwargs[f'request_max_len'] = max_len
        g = self.show_block_graph(record=record, **kwargs)
        # Build graph looks like this: collate({extent}, collate(build_shard, ...)) 
        # and we want the outer collate's arg 1 (inner collate)'s arg 0 -- build_shard 
        if g is None:
            return None
        _ = g.request
        return _
    
    @DEPRECATED # Use show_block_graph()
    def show_block_result(self, *, record=None, max_len=None, **kwargs):
        kwargs[f'request_max_len'] = max_len
        g = self.show_block_graph(record=record, **kwargs)
        # Build graph looks like this: collate({extent}, collate(build_shard, ...)) 
        # and we want the outer collate's arg 1 (inner collate)'s arg 0 -- build_shard 
        if g is None:
            return None
        _ = g.result
        return _

    def show_block_log(self, *, record=None, node=None, **kwargs):
        g = self.show_block_graph(record=record, node=None, **kwargs)
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
            _record = dict(record_schema=Datablock.record_schema,
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

    def build_request(self, alias=None, **scope):
        request = Request(self.build, alias, **scope).apply(self.pool)
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
        if self.show_block_record() is not None:
            _scope = self.show_block_scope()
            if not equal_scopes(blockscope, _scope):
                raise ValueError(f"Attempt to overwrite prior scope {_scope} with {blockscope} for {self.__class__} alias {self.alias}")
        """
        pool_key = utils.datetime_now_key()
        pool_dataspace = self.versionspace.subspace(*(self.pool.anchorchain+(pool_key,))).ensure()
        pool = self.pool.clone(dataspace=pool_dataspace)
        """
        pool = self.pool
        request = self.build_databook_request(pool, **blockscope).with_lifecycle_callback(self._build_databook_request_lifecycle_callback_(**blockscope))
        response = request.evaluate()
        if self.build_echo_task_id:
            print(f"task_id: {response.id}")
        result = response.result()
        return result

    # TODO: build_databook_* -> build_block_*?
    @OVERRIDE
    def build_databook_request(self, pool=None, **scope):
        if pool is None:
            pool = self.pool

        batchscope = self._blockscope_(**scope)
        if self.reload:
            shortfall_databook = self.intent_databook(**batchscope)
        else:
            shortfall_databook = self.shortfall_databook(**batchscope)
        shortfall_databook_kvhandles_lists = [list(shortfall_databook[topic].keys()) for topic in self.topics]
        shortfall_databook_kvhandles_list = [_ for __ in shortfall_databook_kvhandles_lists for _ in __]
        shortfall_databook_kvhandles = list(set(shortfall_databook_kvhandles_list))

        shortfall_tagbatchscope_list = \
            self._kvchains2batches_(*shortfall_databook_kvhandles)
        shortfall_batchscope_list = \
            [{k: batchscope[k] for k in tscope.keys()} for tscope in shortfall_tagbatchscope_list]
        logger.debug(f"Requesting build of shortfall_tagbatch_list: {shortfall_tagbatchscope_list}")
        shortfall_batch_requests = \
            [self._build_batch_request_(shortfall_tagbatchscope_list[i], **shortfall_batchscope_list[i])
                            .apply(pool) for i in range(len(shortfall_tagbatchscope_list))]
        shortfall_batch_requests_str = "[" + \
                                          ", ".join(str(_) for _ in shortfall_batch_requests) + \
                                          "]"
        logger.debug(f"shortfall_batch_requests: " + shortfall_batch_requests_str)
        collated_shortfall_batch_request = \
            Request(self.collate_databooks, *shortfall_batch_requests)
        logger.debug(f"collated_shortfall_batch_request: {collated_shortfall_batch_request}")
        extent_databook = self.extent_databook(**batchscope)
        build_databook_request = \
            Request(self.collate_databooks, extent_databook, collated_shortfall_batch_request)
        return build_databook_request

    @OVERRIDE
    # tagbatchscope is necessary since batchscope will be expanded before being passed to _build_batch_
    def _build_batch_request_(self, tagbatchscope, **batchscope):
        self.versionspace.ensure()
        return Request(self._build_batch_, tagbatchscope, **batchscope)

    @OVERRIDE
    def _build_batch_(self, tagbatchscope, **batchscope):
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
        tagblockscope = self._tagscope_(**blockscope)
        request = Request(self._read_block_, tagblockscope, topic, **blockscope)
        return request
    
    @OVERRIDE
    def _read_block_(self, tagblockscope, topic, **blockscope):
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
            shardspace = self._shardspace_(self.versionspace, topic, **scope)
            """
            if self.verbose:
                print(f"Clearing shardspace {shardspace}")
            """
            logging.debug(f"Clearing shardspace {shardspace}")
            shardspace.remove()


class CachingDatablock(Datablock):
    def __init__(self,
                 dataspace,
                 *,
                 pool=None,
                 lock_pages=True,
                 reload=False,
                 upstreams=(),
                 upstream_raise_on_error=True,
                 upstream_pool=None,
                 upstream_path_filter=None,
                 upstream_reload=False):
        self.upstreams = upstreams
        self.upstream_raise_on_error = upstream_raise_on_error
        self.upstream_pool = upstream_pool
        self.upstream_path_filter = upstream_path_filter
        self.upstream_reload = upstream_reload
        for upstream in self.upstreams:
            upstream.reload = upstream_reload
        super().__init__(dataspace=dataspace,
                         pool=pool,
                         lock_pages=lock_pages,
                         reload=reload)

    def _build_batch_request(self, tagbatchscope, **batchscope):
        upstream_databook_request_dict = \
            {upstream: Request(upstream.build_request, **batch).apply(self.upstream_pool)
             for upstream in self.upstreams}
        copy_upstream_updated_databook_requests = \
            [Request(self._copy_upstream_databook, request, upstream, **batchscope)
             for upstream, request in upstream_databook_request_dict.items()]
        copy_upstream_databook_request = FIRST(copy_upstream_updated_databook_requests)
        return copy_upstream_updated_databook_request

    def _copy_upstream_batch(self, upstream_databook, upstream, **batch):
        logger.debug(f"Loading databook from upstream {upstream}' "
                     f"using scope {batch}")
        if self.versionspace.filesystem.protocol == 'file' or \
                self.versionspace.filesystem.protocol is None:
            tempdirmgr = tempfile.TemporaryDirectory()
        else:
            tempdirmgr = None
        databook = {}
        try:
            for topic, upstream_datapage in upstream_databook:
                datapage = {}
                if topic not in self.topics:
                    raise ValueError(f"Requested topic {topic} not in upstream_databook topics {upstream_databook.keys()}")
                for kvhandle, srcfilepaths in upstream_datapage.items():
                    srcfilepathlist = [srcfilepath for srcfilepath in srcfilepaths if self.upstream_path_filter(srcfilepath)]
                    destdirpath = self._ensure_kvhandle_dirpath(topic, kvhandle)
                    logger.debug(f"Ensured {destdirpath} for kvhandle {kvhandle}")
                    self._lock_kvchain(topic, kvhandle)
                    destfilepathlist = []
                    for srcfilepath in srcfilepathlist:
                        destfilename = self.versionspace.basename(srcfilepath)
                        destfilepath = self.versionspace.join(destdirpath, destfilename)
                        try:
                            logger.debug(f"Copying srcfilepath {srcfilepath} to destfilepath {destfilepath}")
                            if self.versionspace.filesystem.exists(destfilepath):
                                self.versionspace.filesystem.rm(destfilepath)
                            if self.versionspace.filesystem.protocol == 'file':
                                logger.debug(f"Downloading srcfilepath {srcfilepath} directly to {destfilepath}")
                                upstream.versionspace.filesystem.get(srcfilepath, destfilepath)
                            else:
                                tmpfilepath = os.path.join(tempdirmgr.name, destfilename)
                                logger.debug(f"Downloading srcfilepath {srcfilepath} to tmpfilepath {tmpfilepath}")
                                upstream.versionspace.filesystem.get(srcfilepath, tmpfilepath)
                                logger.debug(f"Uploading tmpfilepath {tmpfilepath} to destfilepath {destfilepath}")
                                self.versionspace.filesystem.put(tmpfilepath, destfilepath)
                            destfilepathlist.append(destfilepath)
                        except:
                            excstr = utils.exc_string(*utils.exc_info())
                            errmsg = f"Failed to copy srcfilepath {srcfilepath} to destfilepath {destfilepath}:\n{excstr}\n"
                            raise ValueError(errmsg)
                        finally:
                            self._unlock_kvchain(topic, kvhandle)
                    datapage[kvhandle] = destfilepathlist
                databook[topic] = datapage
        finally:
            if tempdirmgr is not None:
                tempdirmgr.cleanup()
        return databook

    def _ensure_kvhandle_dirpath(self, topic, kvhandle):
        datachain = self._kvchain_to_hvchain_(topic, kvhandle)
        subspace = self.versionspace.subspace(*datachain)
        subspace.ensure()
        return subspace.root


class DB:
    """
        Class containing _datablock members, which instantiate pimp-extensions of Datablock.
        The impl is realized by an instance of cls and
        MAKE_DATABLOCK_CLASS(cls, *, module=__name__, topics, version, use_tempspace=False):
        class DB:
            def __init__(impl_clstr, alias, **kwargs):
                impl_clstr -> cls, module_name
                cls   -> topics, version
                datablock_class = \
                        MAKE_DATABLOCK_CLASS(cls, module_name, topics, version)
                [
                    class datablock_class:
                        def __init__(self, alias, datablock, **impl_kwargs)
                            Datablock.__init__(alias, **datablock)
                            self.impl = cls(**init_kwargs) # >>> impl
                ]
                self._datablock = datablock_class(alias, **kwargs): # >>> pimpl
                    [kwargs get interpreted as datablock, **impl_kwargs]  
        """
    class Request(Request):
        def __init__(self, request, labels):
            self.request = request
            self.labels = labels
        
        def __getattribute__(self, __name: str):
            if __name == '__tag__':
                _ = object.__getattribute__(self, '__tag__')
            else:
                request = object.__getattribute__(self, 'request')
                _ = request.__getattribute__(__name)
            return _
        
        def __tag__(self):
            labels = object.__getattribute__(self, 'labels')
            tag = labels['tag']
            return tag
        
        def __repr__(self):
            labels = object.__getattribute__(self, 'labels')
            repr = labels['repr']
            return repr
        
    
    @staticmethod
    def list_classes(*, dataspace=DATABLOCKS_DATALAKE, pretty_print=True):
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
        
        datablock_dataspace = dataspace.subspace('datablock')

        anchorchains = _chase_anchors(datablock_dataspace)
        datablocks = {'.'.join(anchorchain[:-1]): anchorchain[-1] for anchorchain in anchorchains}
        if pretty_print:
                for key, value in datablocks.items():
                    print(f"{key}: {value}")
        else:
            return datablocks
    
    def __new__(cls, impl_cls_or_clstr, alias=None, *, use_tempspace=False, **kwargs):
        self = object.__new__(cls)
        self._init_(impl_cls_or_clstr, alias, use_tempspace=use_tempspace, **kwargs)
        return self
    
    def _init_(self, impl_cls_or_clstr, alias=None, *, use_tempspace=False, **kwargs):
        if isinstance(impl_cls_or_clstr, str):
            clstr = impl_cls_or_clstr
            clstrparts = clstr.split('.')
            if len(clstrparts) == 1:
                module_name = __name__
                clsname = clstrpargs[0]
            else:
                module_name = '.'.join(clstrparts[:-1])
                clsname = clstrparts[-1]
            mod = importlib.import_module(module_name)
            cls = getattr(mod, clsname)
        else:
            cls = impl_cls_or_clstr
            module_name = cls.__module__
            clstr = f"{module_name}.{cls.__name__}"
        if hasattr(cls, 'topics'):
            topics = cls.topics
        else:
            topics = [DEFAULT_TOPIC]
        if hasattr(cls, 'version'):
            version = cls.version
        else:
            version = DEFAULT_VERSION
        self._repr = Tagger().repr_func(DB, clstr, alias, use_tempspace=use_tempspace, **kwargs)
        if alias is not None:
            self._tag = Tagger(tag_defaults=False).tag_func(DB, clstr, alias, **kwargs)
        else:
            self._tag = Tagger(tag_defaults=False).tag_func(DB, clstr, **kwargs)

        # TODO: could simply save _repr and _tag instead of init_kwargs, alias and clstr
        datablock_class = DB.MAKE_DATABLOCK_CLASS(cls,
                                    module_name=module_name, 
                                    version=version, 
                                    topics=topics, 
                                    use_tempspace=use_tempspace,
                                    )
        self._setup_(cls, alias, datablock_class, **kwargs)
        
    def _setup_(self, cls, alias, datablock_class, **kwargs):
        self._alias = alias
        self._cls = cls
        self._kwargs = kwargs
        self._datablock_class = datablock_class
        self._datablock = datablock_class(alias, **kwargs)
        return self

    def set(self, **kwargs):
        _self = object.__new__(self.__class__)
        _kwargs = copy.deepcopy(self._kwargs)
        _kwargs.update(**kwargs)
        _self._setup_(self._cls, self._alias, self._datablock_class, **_kwargs)
        return _self

    @property
    def pimpl(self):
        _ = object.__getattribute__(self, '_datablock')
        return _
    
    @property
    def impl(self):
        pimpl = self.pimpl
        _ = pimpl.impl
        return _

    @property
    def classname(self): # -> pimpl_classname
        return object.__getattribute__(self, '_cls')
    
    @property
    def datablock(self):
        return self.pimpl
    
    @property
    def alias(self):
        return object.__getattribute__(self, '_alias')
    
    @property
    def scope(self):
        try:
            _scope = object.__getattribute__(self, '_scope')
            return _scope
        except:
            pass
        alias = object.__getattribute__(self, 'alias')
        records = self.datablock.show_block_records()
        if len(records) == 0:
            msg = f"No records for datablock {self.datablock} of version: {self.datablock.version}"
            if self.verbose:
                print(msg)
            raise ValueError(msg)
        if alias is None:
            rec = records.iloc[-1]
        else:
            rec = records[records.alias == alias].iloc[-1]
        # TODO: fix serialization of version to record to exlude `repr`
        if rec['version'] != repr(self.datablock.version) and rec['version'] != self.datablock.version:
            msg = f"Version mismatch for datablock {self.datablock} of version: {self.datablock.version} and record with alias {alias}: {rec['version']}"
            if self.verbose:
                print(msg)
            raise ValueError(msg)
        _scope = _eval(rec['scope'])
        self._scope = _scope
        return _scope
        
    def __repr__(self):
        _ = object.__getattribute__(self, '_repr')
        return _
    
    def __tag__(self):
        _ = object.__getattribute__(self, '_tag')
        return _

    def __hash__(self):
        _repr = repr(self)
        _ =  int(hashlib.sha1(_repr.encode()).hexdigest(), 16)
        return _
    
    def reader(self, topic=None):
        if self.scope is None:
            raise ValueError(f"{self} of version {self.datablock.version} has not been built yet")
        _request = self.pimpl.read_databook_request(topic, **self.scope)\
            .set(summary=lambda _: self.extent()[topic])
        tagger = Tagger(tag_args=True, tag_kwargs=True, tag_defaults=False)
        _funcrepr = f"{self.__tag__()}.reader"
        if topic is None:
            _repr = tagger.repr_func(_funcrepr)
        else:
            _repr = tagger.repr_func(_funcrepr, topic)
        _tag = f"{self.classname}"
        if self.alias is not None:
            _tag += f":{self.alias}"
            if topic is not None:
                _tag += f":{topic}"
        else:
            if topic is not None:
                _tag += f"::{topic}"
        request = self.Request(_request, labels=dict(tag=_tag, repr=_repr))
        return request
    
    def read(self, topic=None):
        if self.scope is None:
                raise ValueError(f"{self} of version {self.pimpl.version} has not been built yet")
        _ = self.pimpl.read(topic, **self.scope)
        return _
    
    def extent(self):
        if self.scope is None:
            raise ValueError(f"{self} of version {self.pimpl.version} has not been built yet")
        _ = self.pimpl.extent(**self.scope)
        return _

    def extent_metric(self):
        if self.scope is None:
            raise ValueError(f"{self} of version {self.pimpl.version} has not been built yet")
        _ = self.datablock.extent_metric(**self.scope)
        return _    

    def UNSAFE_clear(self):
        if self.scope is None:
            raise ValueError(f"{self} of version {self.pimpl.version} has not been built yet")
        _ = self.datablock.UNSAFE_clear(**self.scope)
        return _
    
    # TODO: remove implemented methods from __getattr__
    # TODO: make all name attrs into methods, only the `else-clause` dispatches to self.datablock, the rest can be found in __dict__, obviating the need for __getattr__.
    def __getattr__(self, attrname):
        '''
        if attrname == 'reader':
            if self.scope is None:
                raise ValueError(f"{self} of version {self.datablock.version} has not been built yet")
            def reader(topic=None):
                _request = self.datablock.read_databook_request(topic, **self.scope)\
                    .set(summary=lambda _: self.extent()[topic])
                tagger = Tagger(tag_args=True, tag_kwargs=True, tag_defaults=False)
                _funcrepr = f"{self.__tag__()}.reader"
                if topic is None:
                    _repr = tagger.repr_func(_funcrepr)
                else:
                    _repr = tagger.repr_func(_funcrepr, topic)
                _tag = f"{self.classname}"
                if self.alias is not None:
                    _tag += f":{self.alias}"
                    if topic is not None:
                        _tag += f":{topic}"
                else:
                    if topic is not None:
                        _tag += f"::{topic}"
                request = self.Request(_request, labels=dict(tag=_tag, repr=_repr))
                return request
            return reader
        elif attrname == 'read':
            if self.scope is None:
                raise ValueError(f"{self} of version {self.datablock.version} has not been built yet")
            def read(topic=None):
                _ = self.datablock.read(topic, **self.scope)
                return _
            return read
        elif attrname == 'extent':
            if self.scope is None:
                raise ValueError(f"{self} of version {self.datablock.version} has not been built yet")
            def extent():
                _ = self.datablock.extent(**self.scope)
                return _
            return extent
        elif attrname == 'extent_metric':
            if self.scope is None:
                raise ValueError(f"{self} of version {self.datablock.version} has not been built yet")
            def extent_metric():
                _ = self.datablock.extent_metric(**self.scope)
                return _
            return extent_metric
        elif attrname == 'UNSAFE_clear':
            if self.scope is None:
                raise ValueError(f"{self} of version {self.datablock.version} has not been built yet")
            def UNSAFE_clear():
                _ = self.datablock.UNSAFE_clear(**self.scope)
                return _
            return UNSAFE_clear
        elif attrname == '__tag__':
            _ = _ = object.__getattribute__(self, '__tag__')
            return _
        else:
            attr = getattr(self.datablock, attrname)
            return attr
        '''
        attr = getattr(self.datablock, attrname)
        return attr

    @staticmethod
    def MAKE_DATABLOCK_CLASS(impl_cls, *, module_name=__name__, topics, version, use_tempspace=None, datablock_repr=None, datablock_tag=None):
        """
            Datablock subclass factory using `cls` as implementation of the basic `build()`, `read()`, `valid()`, `metric()` methods.
            Optional members: `version` and `topics`.
            cls must define methods
               build(root|{topic->root}, [fs], **shardscope) -> rooted_shard_path | {topic -> rooted_shard_path}
               read(root|{topic->root}, [fs], **shardscope) -> object | {topic -> object}
        """
         
        def __init__(self, alias=None, datablock={}, **impl_kwargs):
            datablock_kwargs = copy.deepcopy(datablock)
            datablock_kwargs['version'] = version
            Datablock.__init__(self, alias, **datablock_kwargs)
            self.impl_kwargs = impl_kwargs
        
        def __repr__(self):
            if datablock_repr is None:
                repr = Datablock.__repr__(self)
            else:
                repr = datablock_repr
            return repr
        
        def __tag__(self):
            if datablock_repr is None:
                tag = Datablock.__tag__(self)
            else:
                tag = datablock_tag
            return tag

        # REMOVE
        '''
        @classmethod
        def _impl_method_uses_fs(cls, func):
            sig = func_signature(func)
            kinds = [inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD]
            ppars = {name: par for name, par in sig.parameters.items() if par.kind in kinds}
            if len(ppars) > 1:
                key = list(ppars.keys())[1]
                par = ppars[key]
                ann = par.annotation
                if issubclass(ann, fsspec.AbastractFileSystem):
                    return True
            return False

        @classmethod
        def _impl_uses_fs(cls, obj):
            methodnames= ['build', 'read', 'valid', 'metric']
            flag = None
            for methodname in methodnames: 
                method = getattr(obj, methodname)
                _flag = cls._impl_method_uses_fs(method)
                if flag is None:
                    flag = _flag
                else:
                    if flag != _flag:
                        raise ValueError(f"Inconsistent use of filesystem in method {methodname} of object {obj}")
            return flag
        '''
        @functools.lru_cache(maxsize=10)
        def impl(self, root, filesystem):
            try:
                impl = impl_cls(root, filesystem, **self.impl_kwargs)
            except Exception as e:
                print(f"ERROR: failed to instantiate impl_cls {impl_cls} using impl_kwargs {self.impl_kwargs}")
                raise(e)
            return impl

        # REMOVE
        def _load_shardspace(self, topic, **tagshardscope):
            _shardspace = self._shardspace_(self.versionspace, topic, **tagshardscope)
            return _shardspace

        def _build_batch_(self, tagshardscope, **shardscope):
            _dataspace = self.versionspace
                
            fs = _dataspace.filesystem
            if not hasattr(impl_cls, 'topics'):
                topic = None
                _shardspace = self._shardspace_(_dataspace, topic, **tagshardscope)
                impl = self.impl(_shardspace.root, fs)
                if self.verbose:
                        print(f"Building datablock using {impl}")
                impl.build(**shardscope)
                if self.verbose:
                    print(f"Built datablock in _shardspace {_shardspace}")
                logging.debug(f"Built datablock in _shardspace {_shardspace}")
            else:
                _roots = {topic: self._shardspace_(_dataspace, topic, **tagshardscope).root for topic in self.topics}
                
                for topic in self.topics:
                    impl = self.impl(_roots, fs)
                    if self.verbose:
                        print(f"Building datablock using {impl}")
                    impl.build(**shardscope)
                    """
                    if self.verbose:
                        print(f"Built datablock in _shardspace {_shardspace}")
                    """
                    logging.debug(f"Built datablock in _shardspace {_shardspace}")
            _ = self.extent_databook(**tagshardscope)
            return _

        def _read_batch_(self, tagshardscope, topic, **shardscope):
            #_shardspace = self._load_shardspace(topic, **tagshardscope)
            _shardspace = self._shardspace_(self.versionspace, topic, **tagshardscope)
            _root = _shardspace.root
            _fs = _shardspace.filesystem
            if not hasattr(impl_cls, 'topics'):
                _ = self.impl(_root, _fs).read(**shardscope)
            else:
                _ = self.impl(_root, _fs).read(topic, **shardscope)
            return _

        # TODO: pass in tagshardscope *and* shardscope, similar to _build_shard_, etc.
        # TODO: do consistency check on shardpathset.
        def _extent_shard_valid_(self, shardpathset, topic, **scope):
            #_shardspace = self._load_shardspace(topic, **scope)
            _shardspace = self._shardspace_(self.versionspace, topic, **scope)
            _fs = _shardspace.filesystem
            if not hasattr(impl_cls, 'topics'):
                if topic != DEFAULT_TOPIC:
                    raise ValueError(f"Unknown topic: {topic}")
                valid = self.impl(_shardspace.root, _fs).valid(**scope)
            else:
                valid = self.impl(_shardspace.root, _fs).valid(topic, **scope)
            return valid
        
        # TODO: pass in tagshardscope *and* shardscope, similar to _build_batch_, etc.
        def _extent_shard_metric_(self, topic, **scope):
            #_shardspace = self._load_shardspace(topic, **scope)
            _shardspace = self._shardspace_(self.versionspace, topic, **scope)
            _fs = _shardspace.filesystem
            if not hasattr(impl_cls, 'topics'):
                if topic != DEFAULT_TOPIC:
                    raise ValueError(f"Unknown topic: {topic}")
                metric = self.impl(_shardspace.root, _fs).metric(**scope)
            else:
                metric = self.impl(_shardspace.root, _fs).metric(topic, **scope)
            return metric

        scope_defaults = func_kwdefaults(impl_cls.build) 
        scope_params = func_kwonly_parameters(impl_cls.build)
        scope_docstr = ""
        for scope_param in scope_params:
            _scope_docstr = f"\t\t{scope_param}"
            if scope_param in scope_defaults:
                _scope_docstr += f"={scope_defaults[scope_param]}"
            scope_docstr += "\n"+_scope_docstr
        build_docstr = f"scope: {scope_docstr}\n{impl_cls.build.__doc__}"
        #@docstr(build_docstr)

        @functools.wraps(impl_cls.build)
        def build(self, **scope):
            _ = Datablock.build(self, **scope)
            return _

        datablocks_module_name = "DATABLOCK."+module_name
        try:
            importlib.import_module(datablocks_module_name)
        except:
            spec = importlib.machinery.ModuleSpec(datablocks_module_name, None)
            mod = importlib.util.module_from_spec(spec)

        datablock_classdict = {
                    '__module__': datablocks_module_name,
                    'topics': topics,
                    'block_keys': func_kwonly_parameters(impl_cls.build), 
                    'block_defaults': func_kwdefaults(impl_cls.build), 
                    'build': build,
                    'impl': impl,
                    '__init__': __init__,
                    '__repr__': __repr__,
                    '_build_batch_': _build_batch_,
                    '_read_block_': _read_batch_,
                    #'_load_shardspace': _load_shardspace,
        }
        if hasattr(impl_cls, 'valid'):
            datablock_classdict['_extent_shard_valid_'] = _extent_shard_valid_
        if hasattr(impl_cls, 'metric'):
            datablock_classdict['_extent_shard_metric_'] = _extent_shard_metric_

        datablock_class = type(impl_cls.__name__, 
                               (Datablock,), 
                               datablock_classdict,)
        return datablock_class
     
        
