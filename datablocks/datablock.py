import collections
import copy
import datetime
import hashlib
import importlib
import logging
import os
import tempfile
import types

import pdb

import yaml

import pyarrow.parquet
import pyarrow.parquet as pq
import pandas as pd

from . import config, utils
from . import signature as tag
from .signature import Signature, func_kwonly_parameters, func_kwdefaults, ctor_name
from .signature import Tagger
from .utils import DEPRECATED, OVERRIDE, REMOVE, ALIAS, EXTRA, RENAME, BOOL, microseconds_since_epoch, datetime_to_microsecond_str
from .eval.request import Request, Report, FIRST, LAST, ReportSummaryGraph
from .eval.pool import DATABLOCKS_LOGGING_POOL
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
        tagscope = Tagger().tag_dict(**scope)
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

DEFAULT_TOPIC = 'data'
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
      - '_build_batch_(**batchscope)':   
        . write all pages (whole book) for all shards
        . use scope_to_shards to break up the batchscope
        . use [default] '_shardspace_().root' for path(s) to write to
      - ['_shardspace_()']: 
        . used for reading

    OVERRIDE:
    * In case simple impl is insufficient
    * Override priority ranking
      - '_build_batch_(**batchscope)'         
      - '_build_batch_request__(**batchscope)'

      - '_shardspace_(dataspace, topic, **shardscope)': # dataspace should be self.versionspace unless self.tmpspace is being used.
     
      - 'build(**blockscope)'
      - 'build_request(**blockscope)'
      - 'UNSAFE_clear_request(**blockscope)'
    """
    # TODO: implement support for multiple batch_to_shard_keys
    topics = []
    signature = Signature((), ('dataspace', 'version',)) # extract these attrs and use in __tag__

    @staticmethod
    def define(cls, *, module_name=__name__, topics, version, use_local_storage=False):
        """
            cls must define methods
               build(root|{topic->root}, storage_options, **shardscope) -> rooted_shard_path | {topic -> rooted_shard_path}
               read(root|{topic->root}, storage_options, **shardscope) -> object | {topic -> object}
        """
        datablock_classname = cls.__name__
        
        def __init__(self, datablock={}, **kwargs):
            datablock_kwargs = datablock
            datablock_kwargs['version'] = version
            Datablock.__init__(self, **datablock_kwargs)
            cls_kwargs = kwargs
            try:
                self.obj = cls(**cls_kwargs)
            except Exception as e:
                print(f"ERROR: failed to instantiate cls {cls} using cls_kwargs {cls_kwargs}")
                raise(e)
            self.use_local_storage = use_local_storage
            if not self.use_local_storage:
                raise NotImplementedError("Native storage not implemented")

        def _load_shardspace(self, topic, **tagscope):
            if self.use_local_storage:
                shardspace = self._shardspace_(self.versionspace, topic, **tagscope)
                _shardspace = self._shardspace_(self.tmpspace, topic, **tagscope)
                Dataspace.copy(shardspace, _shardspace)
            else:
                _shardspace = self._shardspace_(self.versionspace, topic, **tagscope)
            return _shardspace

        def _build_shard_(self, tagscope, **batchscope):
            # DEBUG
            #print(f"Building shard: {tagscope}")
            # DEBUG
            
            if self.use_local_storage:
                _dataspace = Dataspace.temporary()
            else:
                _dataspace = self.versionspace
                
            storage_options = _dataspace.storage_options
            if len(self.topics) == 1:
                topic = self.topics[0]
                _shardspace = self._shardspace_(_dataspace, topic, **tagscope)
                self.obj.build(_shardspace.root, storage_options, **batchscope)
                if self.use_local_storage:
                    shardspace = self._shardspace_(self.versionspace, topic, **tagscope)
                    if self.verbose:
                        print(f"Copying _shardspace {_shardspace} to shardspace {shardspace}")
                    Dataspace.copy(_shardspace, shardspace)
                else:
                    if self.verbose:
                        print(f"Built datablock in _shardspace {_shardspace}")
            else:
                _roots = {topic: self._shardspace_(_dataspace, topic, **tagscope).root for topic in self.topics}
                self.obj.build(_roots, storage_options, **batchscope)
                for topic in self.topics:
                    if self.use_local_storage:
                            _shardspace = self._shardspace_(_dataspace, topic, **tagscope)
                            shardspace = self._shardspace_(self.versionspace, topic, **tagscope)
                            if self.verbose:
                                print(f"Copying _shardspace {_shardsapce} to shardspace {shardspace}")
                            Dataspace.copy(_shardspace, shardspace)
                    else:
                        if self.verbose:
                            print(f"Built datablock in _shardspace {_shardspace}")
            _ = self.extent_databook(**tagscope)
            return _

        def _read_shard_(self, topic, tagshardscope, **shardscope):
            _shardspace = self._load_shardspace(topic, **tagshardscope)
            _root = _shardspace.root
            _storage_options = _shardspace.storage_options
            if not hasattr(self.obj, 'topics'):
                _ = self.obj.read(_root, _storage_options, **shardscope)
            else:
                _ = self.obj.read(_root, _storage_options, topic, **shardscope)
            return _

        # TODO: pass in tagshardscope *and* shardscope, similar to _build_shard_, etc.
        # TODO: do consistency check on shardpathset.
        def _extent_shard_valid_(self, shardpathset, topic, **tagscope):
            _shardspace = self._load_shardspace(topic, **tagscope)
            if not hasattr(self.obj, 'topics'):
                if topic != DEFAULT_TOPIC:
                    raise ValueError(f"Unknown topic: {topic}")
                valid = self.obj.valid(_shardspace.root, **tagscope)
            else:
                valid = self.obj.valid(_shardspace.root, topic, **tagscope)
            return valid
        
        # TODO: pass in tagshardscope *and* shardscope, similar to _build_batch_, etc.
        def _extent_shard_metric_(self, topic, **tagscope):
            _shardspace = self._load_shardspace(topic, **tagscope)
            if not hasattr(self.obj, 'topics'):
                if topic != DEFAULT_TOPIC:
                    raise ValueError(f"Unknown topic: {topic}")
                metric = self.obj.metric(_shardspace.root, **tagscope)
            else:
                metric = self.obj.metric(_shardspace.root, topic, **tagscope)
            return metric

        datablocks_module_name = "datablock."+module_name
        try:
            importlib.import_module(datablocks_module_name)
        except:
            spec = importlib.machinery.ModuleSpec(datablocks_module_name, None)
            mod = importlib.util.module_from_spec(spec)

        datablock_classdict = {
                    '__module__': datablocks_module_name,
                    'topics': topics,
                    'block_keys': func_kwonly_parameters(cls.build), 
                    'block_defaults': func_kwdefaults(cls.build), 
                    '__init__': __init__,
                    '_build_batch_': _build_shard_,
                    '_read_block_': _read_shard_,
                    '_load_shardspace': _load_shardspace,
        }
        if hasattr(cls, 'valid'):
            datablock_classdict['_extent_shard_valid_'] = _extent_shard_valid_
        if hasattr(cls, 'metric'):
            datablock_classdict['_extent_shard_metric_'] = _extent_shard_metric_

        datablock_class = type(datablock_classname, 
                               (Datablock,), 
                               datablock_classdict,)
        return datablock_class

    # TODO: make dataspace, version position-only and adjust Signature
    def __init__(self,
                 *,
                 dataspace=DATABLOCKS_DATALAKE,
                 version=DEFAULT_VERSION,
                 lock_pages=False,
                 throw=True,
                 rebuild=False,
                 verbose=False,
                 build_echo_task_id=True,
                 pool=DATABLOCKS_LOGGING_POOL):
        Anchored.__init__(self)
        Scoped.__init__(self)
        self.version = version
        self.dataspace = dataspace
        self.anchorspace = dataspace.subspace(*self.anchorchain)
        self.versionspace = self.anchorspace.subspace(str(self.version),)
        self.lock_pages = BOOL(lock_pages)
        self.verbose = BOOL(verbose)
        self.pool = pool
        self.reload = BOOL(rebuild)
        self.throw = BOOL(throw)
        self.build_echo_task_id = BOOL(build_echo_task_id)
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
        tmpspace = self.versionspace.temporary().ensure()
        return tmpspace

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
        tagscope = self._tagscope_(**blockscope)
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        shard_list = self.scope_to_shards(**tagscope)
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
        # DEBUG
        #pdb.set_trace()
        # DEBUG
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
        tagscope = self._tagscope_(**blockscope)
        intent_databook = self.intent_databook(**tagscope)
        _intent_databook = self._databook_kvchain_to_scope(intent_databook)
        return _intent_databook

    def extent(self, **scope):
        blockscope = self._blockscope_(**scope)
        tagscope = self._tagscope_(**blockscope)
        extent_databook = self.extent_databook(**tagscope)
        extent = self._databook_kvchain_to_scope(extent_databook)
        return extent

    def shortfall(self, **scope):
        blockscope = self._blockscope_(**scope)
        tagscope = self._tagscope_(**blockscope)
        shortfall_databook = self.shortfall_databook(**tagscope)
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
        tagscope = self._tagscope_(**blockscope)
        extent = self.extent(**tagscope)
        extent_metric_databook = {}
        for topic, scope_pathset in extent.items():
            if len(scope_pathset) == 2:
                scope, _ = scope_pathset
                shard_metric = self._extent_shard_metric_(topic, **scope)
            else:
                shard_metric = None
            if topic not in extent_metric_databook:
                extent_metric_databook[topic] = []
            extent_metric_databook[topic].append((scope, {'metric': shard_metric}))
        return extent_metric_databook

    def shortfall_databook(self, **scope):
        pagekvhandles_list = [set(self.shortfall_datapage(topic, **scope).keys()) for topic in self.topics]
        bookkvhandleset = set().union(*pagekvhandles_list)
        shortfall_databook = {}
        for topic in self.topics:
            shortfall_datapage = {}
            for kvhandle in bookkvhandleset:
                scope = {key: val for key, val in kvhandle}
                filepathset = self._shardspace_(self.versionspace, topic, **scope).root
                shortfall_datapage[kvhandle] = filepathset
            shortfall_databook[topic] = shortfall_datapage
        return shortfall_databook

    def _shard_pathset_(self, dataspace, topic, **shard):
        shardspace = self._shardspace(dataspace, topic, **shard)
        dirpath = shardspace.root
        return dirpath

    @OVERRIDE
    def _shardspace_(self, dataspace, topic, **shard):
        # TODO: factor **shard through _tagscope_()?
        hvchain = self._scope_to_hvchain_(topic, **shard)
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

    def _scope_to_kvchain_(self, topic, **shard):
        kvhandle = tuple((key, shard[key]) for key in self.shard_keys)
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
    
    RECORDS_COLUMNS_SHORT = ['stage', 'version', 'scope', 'alias', 'task_id', 'metric', 'status', 'date']

    def records(self, *, print=False, full=False, columns=None):
        short = not full
        recordspace = self._recordspace_()
        try:
            parquet_dataset = pq.ParquetDataset(recordspace.root, use_legacy_dataset=False, filesystem=recordspace.filesystem)
            frame = parquet_dataset.read().to_pandas()
        except Exception as e:
            if self.throw:
                raise e
            frame = pd.DataFrame()
        if len(frame) > 0:
            frame.sort_values('timestamp', inplace=True)
            frame.reset_index(inplace=True, drop=True)
            if columns is None:
                _columns = frame.columns
            else:
                _columns = [c for c in columns if c in frame.columns]
            if short:
                _columns_ = [c for c in _columns if c in Datablock.RECORDS_COLUMNS_SHORT]
            else:
                _columns_ = _columns
            _frame = frame[_columns_]
        else:
            _frame = pd.DataFrame()
        
        if print:
            __builtins__['print'](_frame)
        return _frame

    def record_columns(self, *, print=False, short=False):
        frame = self.records(print=False)
        columns = frame.columns
        if short:
            _columns_ = [c for c in columns if c in Datablock.RECORDS_COLUMNS_SHORT]
        else:
            _columns_ = columns
        if print:
            __builtins__['print'](_columns_)
        return _columns_
    
    def record_summary(self, *, record, **kwargs):
        import datablocks
        if isinstance(record, int):
            index = record
            records = self.records(full=True)
            record = records.loc[index]
        summary = record['report_summary']
        return summary

    def record_summary_graph(self, *, record, **kwargs):
        import datablocks
        if isinstance(record, int):
            index = record
            records = self.records(full=True)
            record = records.loc[index]

        logspace = _eval(record['logspace'])
        summary = record['report_summary']
        graph = ReportSummaryGraph(summary, logspace=logspace, **kwargs)
        return graph

    def record_validate_logs(self, *, record, request_max_len=50, **kwargs):
        g = self.record_summary_graph(record=record, **kwargs)
        _ = g.validate_logs(request_max_len=request_max_len)
        return _
    
    def record_log(self, *, record, **kwargs):
        g = self.record_summary_graph(record=record, **kwargs)
        _ = g.log()
        return _

    def list(self):
        records = self.records(full=True)
        _records = records[(records['status']=='STATUS.SUCCEEDED') & (records['stage']=='END')].groupby(['alias', 'version']).last()
        shardscopes = [_shardscope for _shardscope in _records['shardscope']]
        extent_databooks = [self.extent_databook(**shardscope) for shardscope in shardscopes]
        extent_databook = self.collate_databooks(*extent_databooks)
        extent = self._databook_kvchain_to_scope(extent_databook)
        return extent

    def _build_databook_request_lifecycle_callback_(self, alias, **blockscope):
        tagscope = self._tagscope_(**blockscope)
        classname = ctor_name(self.__class__)
        versionstr = repr(self.version)
        tagscopestr = repr(tagscope)
        namestr = f"{classname}:{versionstr}(**{tagscopestr})"
        hashstr = hashlib.sha256(namestr.encode()).hexdigest()[:10]
        if alias is None:
            alias = hashstr
        recordspace = self._recordspace_().ensure()

        def _write_record(lifecycle_stage, request, response):
            timestamp = int(microseconds_since_epoch())
            datestr = datetime_to_microsecond_str()
            _record = dict(alias=alias,
                           stage=lifecycle_stage.name,
                            classname=classname,
                            version=versionstr,
                            scope=blockscope,
                            date=datestr,
                            timestamp=timestamp,
                            task_id=0,
                            status='',
                            success='',
                            metric='',
                            logspace='',
                            logname='',
                            report_summary='',
            )
            if self.verbose:
                    print(f"[BUILD] writing records for request: {request}")
                    print(f"[BUILD] topics: {self.topics}")
                    print(f"[BUILD] blockscope: {blockscope}, tagscope: {tagscope}")
            logname = None
            task_id = None
            if response is not None:
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
                print(f"[BUILD] summary: {summarystr}, logpath: {logpath}, logname: {logname}")
                _record.update(dict(
                                    task_id=task_id,
                                    status=report_summary['status'],
                                    success=report_summary['success'],
                                    logspace=repr(logspace),
                                    logname=logname,
                                    report_summary=summarystr,
                            ))
            records = []
            tagshard_list = self.scope_to_shards(**tagscope)
            for tagshard in tagshard_list:
                for topic in self.topics:
                    record = copy.deepcopy(_record)
                    if response is not None:
                        metric = self._extent_shard_metric_(topic, **tagshard)
                        record['metric'] = str(metric)
                    record['topic'] = topic
                    record['shardscope'] = tagshard
                    records.append(record)
            record_frame = pd.DataFrame.from_records(records)
            record_frame.index.name = 'index'
            record_filepath = \
                recordspace.join(recordspace.root, f"alias-{alias}-stage-{lifecycle_stage.name}-task_id-{task_id}-datetime-{datestr}.parquet")
            if self.verbose:
                print(f"[BUILD] Writing build record at lifecycle_stage {lifecycle_stage.name} to {record_filepath}")
            record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)
        return _write_record

    def build_request(self, alias=None, **scope):
        request = Request(self.build, alias, **scope).apply(self.pool)
        return request

    def build(self, alias=None, **scope):
        # TODO: consistency check: sha256 alias must be unique for a given version or match scope
        blockscope = self._blockscope_(**scope)
        request = self.build_databook_request(**blockscope).with_lifecycle_callback(self._build_databook_request_lifecycle_callback_(alias, **blockscope))
        response = request.evaluate()
        if self.build_echo_task_id:
            print(f"task_id: {response.id}")
        result = response.result()
        return result

    @OVERRIDE
    def build_databook_request(self, **scope):
        batchscope = self._blockscope_(**scope)
        tagbatchscope = self._tagscope_(**batchscope)

        if self.reload:
            shortfall_databook = self.intent_databook(**tagbatchscope)
        else:
            shortfall_databook = self.shortfall_databook(**tagbatchscope)
        shortfall_databook_kvhandles_lists = [list(shortfall_databook[topic].keys()) for topic in self.topics]
        shortfall_databook_kvhandles_list = [_ for __ in shortfall_databook_kvhandles_lists for _ in __]
        shortfall_databook_kvhandles = list(set(shortfall_databook_kvhandles_list))

        shortfall_tagbatchscope_list = \
            self._kvchains2batches_(*shortfall_databook_kvhandles)
        shortfall_batchscope_list = \
            [{k: batchscope[k] for k in tscope.keys()} for tscope in shortfall_tagbatchscope_list]
        logger.debug(f"Requesting build of shortfall_tagbatch_list: {shortfall_tagbatchscope_list}")
        shortfall_batch_requests = \
            [self._build_batch_request_(shortfall_tagbatchscope_list[i], 
                                        **shortfall_batchscope_list[i])
                                                 .apply(self.pool)
                                                 for i in range(len(shortfall_tagbatchscope_list))]
        shortfall_batch_requests_str = "[" + \
                                          ", ".join(str(_) for _ in shortfall_batch_requests) + \
                                          "]"
        logger.debug(f"shortfall_batch_requests: " + shortfall_batch_requests_str)
        collated_shortfall_batch_request = \
            Request(self.collate_databooks, *shortfall_batch_requests)
        logger.debug(f"collated_shortfall_batch_request: {collated_shortfall_batch_request}")
        extent_databook = self.extent_databook(**tagbatchscope)
        build_databook_request = \
            Request(self.collate_databooks, extent_databook, collated_shortfall_batch_request)\
                .apply(self.pool)
        return build_databook_request

    @OVERRIDE
    # TODO: eliminate tagblockscope parameter?  Only needed to form the right _shard_pathset_().  Fix _shard_pathset_ to use (kw)arg tags?
    def _build_batch_request_(self, tagbatchscope, **batchscope):
        self.versionspace.ensure()
        return Request(self._build_batch_, tagbatchscope, **batchscope)

    @OVERRIDE
    def _build_batch_(self, tagbatchscope, **batchscope):
        raise NotImplementedError()

    def read(self, topic, **blockscope):
        _blockscope = self._blockscope_(**blockscope)
        _tagblockscope = self._tagscope_(**_blockscope)
        request = self._read_block_request_(topic, _tagblockscope, **_blockscope)
        response = request.evaluate()
        result = response.result()
        return result

    @OVERRIDE
    # TODO: eliminate tagblockscope parameter?  Only needed to form the right _shard_pathset_().  Fix _shard_pathset_ to use (kw)arg tags?
    def _read_block_request_(self, topic, tagblockscope, **blockscope):
        request = Request(self._read_block_, topic, tagblockscope, **blockscope)
        return request
    
    @OVERRIDE
    def _read_block_(self, topic, tagblockscope, **blockscope):
        raise NotImplementedError()
    
    def UNSAFE_clear_records(self):
        recordspace = self._recordspace_()
        recordspace.remove()
    
    def UNSAFE_clear(self, **scope):
        request = self.UNSAFE_clear_request(**scope)
        _ = request.compute()
        return _

    @OVERRIDE
    def UNSAFE_clear_request(self, **scope):
        blockscope = self._blockscope_(**scope)
        tagblockscope = self._tagscope_(**blockscope)
        request = Request(self._UNSAFE_clear_block_, **tagblockscope)
        return request
    
    @OVERRIDE
    def _UNSAFE_clear_block_(self, **scope):
        for topic in self.topics:
            shardspace = self._shardspace_(self.versionspace, topic, **scope)
            if self.verbose:
                print(f"Clearing shardspace {shardspace}")
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

"""
    Wrapper around
    def define(cls, *, module=__name__, topics, version, use_local_storage=False):  
"""
def DATABLOCK(cls_or_clstr, use_native_storage=False, **init_kwargs):
    if isinstance(cls_or_clstr, str):
        clstr = cls_or_clstr
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
        cls = cls_or_clstr
        module_name = cls.__module__.__name__
    if hasattr(cls, 'topics'):
        topics = cls.topics
    else:
        topics = [DEFAULT_TOPIC]
    if hasattr(cls, 'version'):
        version = cls.version
    else:
        version = DEFAULT_VERSION
    datablock_class = Datablock.define(cls, module_name=module_name, version=version, topics=topics, use_local_storage=not use_native_storage)
    datablock_obj = datablock_class(**init_kwargs)
    return datablock_obj
    

class READ(Request):
    def __init__(self, argstr):
        self.argstr = argstr
        argparts = argstr.split('@')
        if len(argparts) > 2:
            raise ValueError(f"Malformed datablock reference: {argstr}")
        if len(argparts) == 1:
            alias = None
            classname_topic = argstr
            classname_topic_parts = classname_topic.split(':')
            if len(classname_topic_parts) > 2:
                raise ValueError(f"Malformed datablock reference: {argstr}")
            elif len(classname_topic_parts) == 2:
                classname, topic = tuple(classname_topic_parts)
            else:
                classname = classname_topic_parts[0]
                topic = None
            #DEBUG
            #print(f"Parsed classname_topic {classname_topic} into {classname} and {topic}")
            #DEBUG
        else:
            classname, alias_topic = argparts
            alias_topic_parts = alias_topic.split(':')
            if len(alias_topic_parts) > 2:
                raise ValueError(f"Malformed datablock reference: {argstr}")
            elif len(alias_topic_parts) == 2:
                alias, topic = tuple(alias_topic_parts)
            else:
                alias = alias_topic_parts[0]
                topic = None

            #DEBUG
            #print(f"Parsed classname_alias_topic {argstr} into {classname}, {alias} and {topic}")
            #DEBUG

        obj = DATABLOCK(classname)
        """
        classpath = classname
        classparts = classpath.split('.')
        modname = '.'.join(classparts[:-1])
        clsname = classparts[-1]
        #DEBUG
        #print(f"Parsed classpath into modname: {modname} and clsname: {clsname}")
        #DEBUG

        mod = importlib.import_module(modname)
        cls = getattr(mod, clsname)
        obj = cls()
        """
        func = getattr(obj, 'read')

        records = obj.records()
        if len(records) == 0:
            raise ValueError(f"No records for datablock {obj} of classname {classname}: version: {obj.version}")
        if alias is None:
            rec = records.iloc[-1]
        else:
            rec = records[records.alias == alias].iloc[-1]
        # TODO: fix serialization of version to record to exlude `repr`
        if rec['version'] != repr(obj.version) and rec['version'] != obj.version:
            raise ValueError(f"Version mismatch for datablock {obj} of classname {classname}: version: {obj.version} and record with alias {alias}: {rec['version']}")
        scope = _eval(rec['scope'])
       
        if topic is None:
            args = [obj.topics[0]]
        else:
            args = [topic]
        Request.__init__(self, func, *args, **scope)
    
    def __tag__(self):
        return self.argstr

    def __hash__(self):
        hashstr = hashlib.sha1(self.argstr.encode()).hexdigest()
        hsh = int(hashstr, 16)
        return hsh


def PRINT(argstr):
    request = READ(argstr)
    _ = request.compute()
    print(_)