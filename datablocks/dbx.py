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
from typing import Any, TypeVar, Generic, Tuple, Union, List, Dict

import fsspec


import pyarrow.parquet as pq
import pandas as pd


from . import signature
from .signature import Signature, ctor_name
from .signature import tag, Tagger
from .utils import ALIAS, DEPRECATED, OVERRIDE, microseconds_since_epoch, datetime_to_microsecond_str
from .eval import request, pool
from .eval.request import Request, Report, LAST, NONE, Graph
from .eval.pool import DATABLOCKS_STDOUT_LOGGING_POOL, DATABLOCKS_FILE_LOGGING_POOL
from .dataspace import DATABLOCKS_DATALAKE, DATABLOCKS_HOMELAKE


def _eval(string):
    """
        A version of __builtins__.eval() in the context of impoirted datablocks.datablock.
    """
    import datablocks.dbx
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
    REVISION = '0.0.1'
    FILENAME = None # define or define TOPICS: dict

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
                    path = os.join(os.getcwd(), self.FILENAME)
                else:
                    path_ = roots
                    os.makedirs(path_, exist_ok=True)
                    path = os.path.join(path_, self.FILENAME)
            else:
                path = roots + "/" + self.FILENAME
        return path

    def print_verbose(self, s):
        if self.verbose:
            print(f">>> {self.__class__.__qualname__}: {s}")

    def print_debug(self, s):
        if self.debug:
            print(f"DEBUG: >>> {self.__class__.__qualname__}: {s}")


class Anchored:
    def __init__(self, namechain=None):
        self.namechain = namechain

    @property
    def anchorchain(self):
        if not hasattr(self, 'anchor'):
            modchain = tuple(str(self.__class__.__module__).split('.'))
            anchorclassname = self.__class__.__qualname__.split('.')[-1]
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
                if not isinstance(val, RANGE):
                    _val = RANGE([val]) 
                else:
                    _val = val
            elif key in self.block_keys:
                _key, _val = key, val
            else:
                raise ValueError(f"key={key} with val={val} is neither a block key nor a shard key")
            return _key, _val
        _blockscope = {}
        for key, val in scope.items():
            k, v = blockify(key, val)
            _blockscope[k] = v
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
                raise ValueError(f"MISSING scope argument '{key}'? Details: block_key={key} is neither in blockified _blockscope={_blockscope} "
                                 f"nor in block_defaults={self.block_defaults}, or in block_pins={self.block_pins}")
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
DEFAULT_REVISION = '0.0.0'

class Databuilder(Anchored, Scoped): 
    # TODO: implement support for multiple batch_to_shard_keys
    revision = DEFAULT_REVISION
    topics = [DEFAULT_TOPIC]
    signature = Signature((), ('dataspace', 'revision',)) # extract these attrs and use in __tag__

    # TODO: make dataspace, revision position-only and adjust Signature
    def __init__(self,
                 alias=None,
                 *,
                 dataspace=DATABLOCKS_HOMELAKE,
                 tmpspace=None,
                 lock_pages=False,
                 throw=True,
                 rebuild=False,
                 pool=DATABLOCKS_STDOUT_LOGGING_POOL,
                 build_block_request_lifecycle_callback=None,
                 verbose=False,
                 debug=False,          
    ):
        Anchored.__init__(self)
        Scoped.__init__(self)
        self.alias = alias
        self.dataspace = dataspace
        self._tmpspace = tmpspace
        self.anchorspace = dataspace.subspace(*self.anchorchain)
        #self.revisionspace = self.anchorspace.subspace(f"revision={str(self.revision)}",)
        self.lock_pages = lock_pages
        self.throw = throw
        self.reload = rebuild
        self.pool = pool
        self.build_block_request_lifecycle_callback = build_block_request_lifecycle_callback
        self.verbose = verbose
        self.debug = debug
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
    def revisionspace(self):
        return self.anchorspace.subspace(f"revision={str(self.revision)}",)

    @property
    def tmpspace(self):
        if self._tmpspace is None:
            self._tmpspace = self.revisionspace.temporary(self.revisionspace.subspace('tmp').ensure().root)
        return self._tmpspace

    def block_intent_pathpage(self, topic, **scope):
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
        block_intent_pathpage = {kvhandle: pathshard for kvhandle, pathshard in kvhandle_pathshard_list}
        return block_intent_pathpage

    @OVERRIDE
    def _shard_extent_pathpage_valid_(self, topic, tagscope, **shardscope):
        pathset = self._shardspace_(topic, **tagscope).root
        valid = self.revisionspace.filesystem.isdir(pathset)
        if self.debug:
            if valid:
                print(f"_shard_extent_pathpage_valid_: VALID: shard with topic {repr(topic)} with scope with tag {tagscope}")
            else:
                print(f"_shard_extent_pathpage_valid_: INVALID shard with topic {repr(topic)} with scope with tag {tagscope}")
        return valid

    def block_extent_pathpage(self, topic, **scope):
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        block_intent_pathpage = self.block_intent_pathpage(topic, **scope)
        block_extent_pathpage = {}
        for kvhandle, shard_pathset in block_intent_pathpage.items():
            shardscope = self._kvhandle_to_scope_(topic, kvhandle)
            tagscope = self._tagscope_(**shardscope)
            valid = self._shard_extent_pathpage_valid_(topic, tagscope, **shardscope)
            if valid:
                block_extent_pathpage[kvhandle] = shard_pathset
        return block_extent_pathpage
    
    def block_extent_pathpage_request(self, topic, **scope):
        _ = Request(self.block_extent_pathpage, topic, **scope)
        return _

    def block_shortfall_pathpage(self, topic, **scope):
        block_intent_pathpage = self.block_intent_pathpage(topic, **scope)
        block_extent_pathpage = self.block_extent_pathpage(topic, **scope)
        block_shortfall_pathpage = {}
        for kvhandle, intent_pathshard in block_intent_pathpage.items():
            if isinstance(intent_pathshard, str):
                if kvhandle not in block_extent_pathpage or block_extent_pathpage[kvhandle] != intent_pathshard:
                    shortfall_pathshard = intent_pathshard
                else:
                    shortfall_pathshard = []
            else:
                if kvhandle not in block_extent_pathpage:
                    shortfall_pathshard = intent_pathshard
                else:
                    extent_pathshard = block_extent_pathpage[kvhandle]
                    shortfall_pathshard = [intent_filepath for intent_filepath in intent_pathshard
                                             if intent_filepath not in extent_pathshard]
            if len(shortfall_pathshard) == 0:
                continue
            block_shortfall_pathpage[kvhandle] = shortfall_pathshard
        return block_shortfall_pathpage

    #REMOVE?
    def block_intent_pathbook(self, **scope):
        return self._page_pathbook("intent", **scope)

    #REMOVE?
    def block_extent_pathbook(self, **scope):
        return self._page_pathbook("extent", **scope)

    @staticmethod
    def collate_pathpages(topic, *pathpages):
        # Each pathpage is a dict {kvhandle -> filepathset}.
        # `collate` is just a union of dicts.
        # Assuming each kvhandle exists only once in pathpages or only the last occurrence matters.
        collated_pathpage = {kvhandle: filepathset for pathpage in pathpages for kvhandle, filepathset in pathpage.items()}
        return collated_pathpage

    @staticmethod
    def collate_pathbooks(*pathbooks):
        # Collate all pages from all books within a topic
        topic_pathpages = {}
        for pathbook in pathbooks:
            for topic, pathpage in pathbook.items():
                if topic in topic_pathpages:
                    topic_pathpages[topic].append(pathpage)
                else:
                    topic_pathpages[topic] = [pathpage]
        collated_pathbook = \
            {topic: Databuilder.collate_pathpages(topic, *pathpages) \
             for topic, pathpages in topic_pathpages.items()}
        return collated_pathbook

    def block_intent(self, **scope):
        blockscope = self._blockscope_(**scope)
        tagscope = self._tagscope_(**blockscope)
        block_intent_pathbook = self.block_intent_pathbook(**tagscope)
        _block_intent_pathbook = self._pathbook_to_scopebook(block_intent_pathbook)
        return _block_intent_pathbook

    def block_extent(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        block_extent_pathbook = self.block_extent_pathbook(**blockscope)
        extent = self._pathbook_to_scopebook(block_extent_pathbook)
        return extent

    def block_shortfall(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        block_shortfall_pathbook = self.block_shortfall_pathbook(**blockscope)
        _block_shortfall_pathbook = self._pathbook_to_scopebook(block_shortfall_pathbook)
        return _block_shortfall_pathbook

    #RENAME: -> _shard_extent_metric_
    def _extent_shard_metric_(self, topic, tagscope, **shardscope):
        block_extent_pathpage = self.block_extent_pathpage(topic, **shardscope)
        if self.debug:
            print(f"_extent_shard_metric_: topic: {repr(topic)}: shardspace: {shardscope}: block_extent_pathpage: {block_extent_pathpage}")
        if len(block_extent_pathpage) > 1:
            raise ValueError(f"Too many shards in block_extent_pathpage: {block_extent_pathpage}")
        if len(block_extent_pathpage) == 0:
            metric = 0
        else:
            pathset = list(block_extent_pathpage.values())[0]
            if isinstance(pathset, str):
                metric = 1
            else:
                metric = len(pathset)
        return metric
    
    def block_extent_metric(self, **scope):
        blockscope = self._blockscope_(**scope)
        tagscope = self._tagscope_(**blockscope)
        extent = self.block_extent(**blockscope)
        extent_metric_pathbook = {}
        #FIX: break blockscope into shards: see logic in block_extent_pathpage()
        for topic, scope_pathset in extent.items():
            if len(scope_pathset) == 2:
                scope, _ = scope_pathset
                shard_metric = self._extent_shard_metric_(topic, tagscope, **blockscope)
            else:
                shard_metric = None
            if topic not in extent_metric_pathbook:
                extent_metric_pathbook[topic] = []
            extent_metric_pathbook[topic].append((scope, {'metric': shard_metric}))
        return extent_metric_pathbook

    def block_shortfall_pathbook(self, **scope):
        blockscope = self._blockscope_(**scope)
        pagekvhandles_list = [set(self.block_shortfall_pathpage(topic, **blockscope).keys()) for topic in self.topics]
        bookkvhandleset = set().union(*pagekvhandles_list)
        block_shortfall_pathbook = {}
        for topic in self.topics:
            block_shortfall_pathpage = {}
            for kvhandle in bookkvhandleset:
                scope = {key: val for key, val in kvhandle}
                filepathset = self._shardspace_(topic, **blockscope).root
                block_shortfall_pathpage[kvhandle] = filepathset
            block_shortfall_pathbook[topic] = block_shortfall_pathpage
        return block_shortfall_pathbook
    
    @OVERRIDE
    def _shardspace_(self, topic, **shard):
        #TODO: ensure topic=None is handled correctly: `topic == None` must only be allowed when `self.topics == None` 
        #TODO: disallow the current default `self.topics == [None]` -> `self.topic = None`
        #TODO: when `self.topics is not None` it must be a list of valid str
        #TODO: `self.topics == None` must mean that `_shardspace_` generates a unique space corresponding to an `hvhandle` with no topic head
        #TODO: `scope_to_hvhandle` with topic==None must generate an hvhandle with no topic head
        tagshard = self._tagscope_(**shard)
        hvhandle = self._scope_to_hvhandle_(topic, **tagshard)
        subspace = self.revisionspace.subspace(*hvhandle)
        if self.debug:
            print(f"SHARDSPACE: formed for topic {repr(topic)} and shard with tag {tagshard}: {subspace}")
        return subspace
    
    class _scopepage_(tuple):
        def __repr__(self):
            if len(self) < 2:
                assert len(self) == 0
                return ''
            scope, pathset = self[0], self[1]
            if len(scope) > 0:
                _ = str(pathset)
            else:
                _ = str(scope) + ": " + str(pathset)
            return _

    class _scopebook_(dict):
        def __repr__(self):
            lines = []
            for topic, page in self.items():
                if topic is None:
                    lines.append(repr(page))
                else:
                    lines.append(repr(topic) + ": " + repr(page))
            _ = '\n'.join(lines)
            return _

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
        subspace = self.revisionspace.subspace(*datachain)
        path = subspace.root
        return path
    
    def _pathbook_to_scopebook(self, pathbook):
        _scopebook = {}
        for topic, pathpage in pathbook.items():
            if not topic in _scopebook:
                _scopebook[topic] = ()
            for kvhandle, pathset in pathpage.items():
                scope = self._kvhandle_to_scope_(topic, kvhandle)
                _scopebook[topic] += (scope, pathset)
        _scopebook_ = Databuilder._scopebook_({topic: Databuilder._scopepage_(page) for topic, page in _scopebook.items()}) 
        return _scopebook_

    def _lock_kvhandle(self, topic, kvhandle):
        if self.lock_pages:
            hivechain = self._kvhandle_to_hvhandle_(topic, kvhandle)
            self.revisionspace.subspace(*hivechain).acquire()

    def _unlock_kvhandle(self, topic, kvhandle):
        if self.lock_pages:
            hivechain = self._kvhandle_to_hvhandle_(topic, kvhandle)
            self.revisionspace.subspace(*hivechain).release()

    #REMOVE: unroll inplace where necessary
    def _page_pathbook(self, domain, **kwargs):
        # This pathbook is computed by computing a page for each topic separately, 
        # via a dedicated function call with topic as an arg, using a domain-specific
        # method.  domain: 'intent'|'extent'|'shortfall'
        _pathpage = getattr(self, f"block_{domain}_pathpage")
        pathbook = {topic: _pathpage(topic, **kwargs) for topic in self.topics}
        return pathbook

    @OVERRIDE
    def build_pathbook_request(self, **scope):
        blockscope = self._blockscope_(**scope)
        if self.reload:
            block_shortfall_pathbook = self.block_intent_pathbook(**blockscope)
        else:
            block_shortfall_pathbook = self.block_shortfall_pathbook(**blockscope)
        block_shortfall_pathbook_kvhandles_lists = [list(block_shortfall_pathbook[topic].keys()) for topic in self.topics]
        block_shortfall_pathbook_kvhandles_list = [_ for __ in block_shortfall_pathbook_kvhandles_lists for _ in __]
        block_shortfall_pathbook_kvhandles = list(set(block_shortfall_pathbook_kvhandles_list))

        shortfall_batchscope_list = \
            self._kvhandles_to_batches_(*block_shortfall_pathbook_kvhandles)
        shortfall_batchscope_list = \
            [{k: blockscope[k] for k in tscope.keys()} for tscope in shortfall_batchscope_list]
        if self.verbose:
            if len(shortfall_batchscope_list) == 0:
                print(f"Databuilder: build_pathbook_request: no shortfalls found: returning extent")
            else:
                print(f"Databuilder: build_pathbook_request: requesting build of shortfall batchscopes with tags: {shortfall_batchscope_list}")
        _shortfall_batch_requests = \
            [self._build_batch_request_(self._tagscope_(**shortfall_batchscope_list[i]), **shortfall_batchscope_list[i])
                            .apply(self.pool) for i in range(len(shortfall_batchscope_list))]
        shortfall_batch_requests = [_.apply(self.pool) for _ in _shortfall_batch_requests]
        shortfall_batch_requests_tags = "[" + \
                                          ", ".join(tag(_) for _ in shortfall_batch_requests) + \
                                          "]"
        if self.verbose:
            print(f"build_pathbook_request: shortfall_batch_requests: " + shortfall_batch_requests_tags)

        tagscope = self._tagscope_(**blockscope)
        extent_request = Request(self.block_extent, **tagscope)
        requests = shortfall_batch_requests + [extent_request]
        build_pathbook_request = LAST(*requests)
        #TODO: #FIX
        #build_pathbook_request = LAST(*shortfall_batch_requests) if len(shortfall_batch_requests) > 0 else NONE()
        if len(shortfall_batchscope_list) > 0 and self.build_block_request_lifecycle_callback is not None:
            _ = build_pathbook_request.with_lifecycle_callback(self.build_block_request_lifecycle_callback(**blockscope))
            if self.verbose:
                print(f"Databuilder: build_pathbook_request: will record lifecycle")
        else:
            _ = build_pathbook_request
            if self.verbose:
                print(f"Databuilder: build_pathbook_request: will NOT record lifecycle")
        return _

    @OVERRIDE
    # tagscope is necessary since batchscope will be expanded before being passed to _build_batch_
    def _build_batch_request_(self, tagscope, **batchscope):
        self.revisionspace.ensure()
        return Request(self._build_batch_, tagscope, **batchscope)

    @OVERRIDE
    def _build_batch_(self, tagscope, **batchscope):
        raise NotImplementedError()

    @OVERRIDE
    # tagbatchscope is necessary since batchscope will be expanded before being passed to _read_block_
    def read_block_pathpage_request(self, topic, **blockscope):
        _blockscope = self._blockscope_(**blockscope)
        _tagscope = self._tagscope_(**_blockscope)
        request = Request(self._read_block_, _tagscope, topic, **blockscope)
        return request
    
   
class DBX:
    """
        DBX instantiates a Databuilder class on demand, in particular, different instances depending on the build(**kwargs),
            . pool=Ray()
            . etc
        Not inheriting from Databuilder also has the advantage of hiding the varable scope API that Databuilder.build/read/etc presents.
        DBX is by definition a fixed scope block.
    """

    RECORD_SCHEMA_REVISION = '0.3.0'

    class Request(request.Proxy):
        @staticmethod
        def parse_url(url: str, **datablock_kwargs):
            #url = {classtr}@{revision}|{alias}:{topic}
            tail = url
            try:
                datablock_clstr, tail  = tail.split("@")
            except:
                raise ValueError(f"Malformed url: {url}")
            try:
                revision, tail  = tail.split("#")
            except:
                raise ValueError(f"Malformed url: {url}")
            try:
                alias, topic  = tail.split(":")
                if len(alias) == 0:
                    alias = None
                if len(topic) == 0:
                    topic = DEFAULT_TOPIC
            except:
                raise ValueError(f"Malformed url: {url}")
            dbx = DBX(datablock_clstr, alias, **datablock_kwargs)
            return dbx, revision, topic

        def __init__(self, url=None, *, dbx, revision, topic, pic=False):
            self.url = url
            if self.url is None:
                self.dbx, self.revision, self.topic = dbx, revision, topic
            else:
                self.dbx, revision, self.topic = self.parse_url(url)
            self.pic = pic
            self.dbx = self.dbx.with_pic(pic=pic)
            if hasattr(self.dbx.datablock_cls, 'TOPICS'):
                if topic not in self.dbx.datablock_cls.TOPICS:
                    raise ValueError(f"Topic {topic} not from among {self.dbx.datablock_cls.TOPICS}")
            if not pic:
                if dbx.databuilder.revision != revision:
                    raise ValueError(f"Incompatible revision {revision} from url for dbx {dbx}")
            else:
                if dbx.databuilder.revision != revision.format and\
                    revision == f"{dbx.__class__.__qualname__}.REVISION":
                        raise ValueError(f"Incompatible revision {revision} from url for dbx {dbx}")

        def with_pic(self, pic=True):
            _ = self.__class__(self.url, dbx=self.dbx, revision=self.revision, topic=self.topic, pic=pic)
            return _

        def __ne__(self, other):
            # .request might fail due to previous build record mismatch, 
            # so treat that as a failure of equality
            try:
                _ =  not isinstance(other, self.__class__) or \
                    repr(self.request) != repr(other.request) #TODO: implemented a more principled Request __ne__
            except:
                _ = True
            return _
        
        def __eq__(self, other):
            return not self.__ne__(other)
        
        def __str__(self):
            _ = self.__repr__()
            return _ 
        
        def __repr__(self):
            if self.url is not None:
                _ = Tagger(tag_defaults=False).repr_ctor(self.__class__, self.url)
            else:
                _ = Tagger(tag_defaults=False).repr_ctor(self.__class__, dbx=self.dbx, revision=self.revision, topic=self.topic)
            return _

    class Reader(Request):
        @property
        def request(self):
            _ = self.dbx.read_request(topic=self.topic)
            return _
        
        def __tag__(self):
            _tag__ = f"{DBX_PREFIX}.{self.dbx.datablock_clstr}"
            tag__ = _tag__+f"@{self.dbx.databuilder.revision}"
            tag_ =  tag__+f"#{self.dbx.alias}" if self.dbx.alias is not None else tag__+"#"
            tag = tag_ + f":{self.topic}" if self.topic != DEFAULT_TOPIC else tag_+":"
            return tag
    
    class Pather(Reader):
        @property
        def request(self):
            _ = self.dbx.path_request(topic=self.topic)
            return _

        def __tag__(self):
            _tag = super().__tag__()
            tag = f"[{_tag}]"
            return tag

    class Builder(request.Proxy):
        pass

    class Pool(pool.Logging):
        def __init__(self, pool):
            self.pool = pool

    @staticmethod
    def show_datablocks(*, dataspace=DATABLOCKS_HOMELAKE, pretty_print=True):
        def _chase_anchors(_dataspace, _anchorchain=()):
            filenames = _dataspace.list()
            anchorchains = []
            for filename in filenames:
                if filename.startswith('.'):
                    continue
                elif dataspace.isfile(filename):
                    continue
                elif filename.startswith('revision='):
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
                dataspace=DATABLOCKS_DATALAKE,
                tmpspace=None,
                lock_pages=False,
                throw=True,
                rebuild=False,
                pool=DATABLOCKS_STDOUT_LOGGING_POOL,
                verbose=False,
                debug=False,  
                pic=False,
                alias_dataspace=False,
    ):
        self.alias = alias
        self.debug = debug
        self.verbose = verbose
        self.pic = pic
        self.alias_dataspace = alias_dataspace

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
            dataspace=dataspace,
            pool=pool,
            tmpspace=tmpspace, # derive from dataspace?
            lock_pages=lock_pages,
            throw=throw, # fold into `pool`?
            rebuild=rebuild, # move to build()?
            verbose=verbose,
            debug=debug,
        )
        @functools.wraps(Databuilder)
        def update_databuilder_kwargs(**kwargs):
            self.databuilder_kwargs.update(**kwargs)
            return self
        self.Databuilder = update_databuilder_kwargs

    def clone(self, 
              *,
              dataspace=None,
              tmpspace=None,
              lock_pages=None,
              throw=None,
              rebuild=None,
              pool=None,
              verbose=None,
              debug=None,  
              pic=None,
              alias_dataspace=None,
    ):
        kwargs = copy.copy(self.databuilder_kwargs)
        kwargs['pic'] = self.pic
        kwargs['alias_dataspace'] = self.alias_dataspace
        if dataspace is not None:
            kwargs['dataspace'] = dataspace
        if tmpspace is not None:
            kwargs['tmpspace'] = tmpspace
        if lock_pages is not None:
            kwargs['lock_pages'] = lock_pages
        if throw is not None:
            kwargs['throw'] = throw
        if rebuild is not None:
            kwargs['rebuild'] = rebuild
        if pool is not None:
            kwargs['pool'] = pool
        if debug is not None:
            kwargs['debug'] = debug
        if verbose is not None:
            kwargs['verbose'] = verbose
        if pic is not None:
            kwargs['pic'] = pic
        if alias_dataspace is not None:
            kwargs['alias_dataspace'] = alias_dataspace

        _ = DBX(self.datablock_cls, 
                self.alias,
                **kwargs)

        if self._scope is not None:
            _scope = {}
            for key, val in self._scope.items():
                if isinstance(val, DBX.Reader) or isinstance(val, DBX.Pather):
                    _scope[key] = val.with_pic(pic)
                else:
                    _scope[key] = val
            _._scope = _scope
        else:
            _._scope = self._scope

        _.datablock_kwargs = self.datablock_kwargs
        return _

    def with_pic(self, pic=True):
        _ = self.clone(pic=pic)
        return _

    def __repr__(self):
        """
        if self.alias is not None:
            _ =  Tagger().repr_func(self.__class__, self.datablock_clstr, self.alias)
        else:
            _ = Tagger().repr_func(self.__class__, self.datablock_clstr)
        """
        #FIX: do not make default self.alias None explicit
        _ =  Tagger(tag_defaults=False).repr_ctor(self.__class__, self.datablock_clstr, self.alias, pic=self.pic,)
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

    @property
    def databuilder(self):
        databuilder_cls = self._make_databuilder_class()
        databuilder_kwargs = self.databuilder_kwargs
        databuilder_kwargs['dataspace'] = databuilder_kwargs['dataspace'].with_pic(True)
        databuilder = databuilder_cls(self.alias, 
                                      build_block_request_lifecycle_callback=self._build_block_request_lifecycle_callback_,
                                      **databuilder_kwargs)
        return databuilder

    @property
    @functools.lru_cache(maxsize=None)
    def scope(self):
        if self._scope is not None:
            _scope = self._scope
            if self.verbose:
                print(f"DBX: scope: databuilder {self.databuilder} with revision {self.databuilder.revision} with alias {repr(self.databuilder.alias)}: using specified scope: {self._scope}")
        else:
            record = self.show_named_record(alias=self.databuilder.alias, revision=self.databuilder.revision, stage='END') 
            if record is not None:
                _scope = _eval(record['scope'])
                if self.verbose:
                    print(f"DBX: scope: no specified scope for databuilder {self.databuilder} with revision {self.databuilder.revision} with alias {repr(self.databuilder.alias)}: using build record scope: {_scope}")
            else:
                _scope = {}
                if self.verbose:
                    print(f"DBX: scope: no specified scope and no records for databuilder {self.databuilder} with revision {self.databuilder.revision} with alias {repr(self.databuilder.alias)}: using default scope")
        return _scope
    
    def build(self):
        request = self.build_request()
        response = request.evaluate()
        if self.verbose:
            print(f"task_id: {response.id}")
        result = response.result()
        return result

    def build_request(self):
        import datablocks
        def scopes_equal(s1, s2):
            if set(s1.keys()) != (s2.keys()):
                return False
            for key in s1.keys():
                if s1[key] != s2[key]:
                    return False
            return True
        # TODO: consistency check: sha256 alias must be unique for a given revision or match scope
        blockscope = self.databuilder._blockscope_(**self.scope)
        record = self.show_named_record(alias=self.databuilder.alias, revision=self.databuilder.revision, stage='END') 
        if record is not None:
            try:
                _scope = _eval(record['scope'])
            except Exception as e:
                # For example, when the scope contains tags of DBX with an earlier revision.
                if self.verbose:
                    print(f"Failed to retrieve scope from build record, ignoring scope of record.")
                _scope = None
            if _scope is not None and not scopes_equal(blockscope, _scope):
                raise ValueError(f"Attempt to overwrite prior scope {_scope} with {blockscope} for {self.__class__} alias {self.alias}")
        request = self.databuilder.build_pathbook_request(**blockscope)
        return request
    
    def display(self):
        _ = self.databuilder.display_batch(**self.scope)
        return _ 
    
    def path(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"{self} of revision {self.revision} has not been built yet")
        pathpage = self.databuilder.block_extent_pathpage(topic, **self.tagscope)
        if len(pathpage) > 1:
            path = list(pathpage.values())
        elif len(pathpage) == 1:
            path = list(pathpage.values())[0]
        else:
            path = None
        return path
    
    def path_request(self, topic=DEFAULT_TOPIC):
        _ = request.Request(self.path, topic)
        return _

    def read_request(self, topic=DEFAULT_TOPIC):
        if self.scope is None:
            raise ValueError(f"{self} of revision {self.revision} has not been built yet")
        request = self.databuilder.read_block_pathpage_request(topic, **self.scope)\
            .set(summary=lambda _: self.extent()[topic]) # TODO: #REMOVE?
        return request

    def PATH(self, topic=None):
        _ = DBX.Pather(dbx=self, revision=self.databuilder.revision, topic=topic, pic=self.pic)
        return _

    def READ(self, topic=None):
        _ = DBX.Reader(dbx=self, topic=topic, revision=self.databuilder.revision, pic=self.pic)
        return _
    
    def read(self, topic=DEFAULT_TOPIC):
        read_request = self.read_request(topic)
        result = read_request.compute()
        return result
    
    def intent(self):
        _ = self.databuilder.block_intent(**self.scope)
        return _

    def extent(self):
        _ = self.databuilder.block_extent(**self.scope)
        return _
    
    def extent_metric(self):
        _ = self.databuilder.block_extent_metric(**self.scope)
        return _    
    
    def valid(self):
        _ = self.extent()
        return _

    def metric(self, topic=None):
        _ = self.extent_metric()
        return _
    
    @property
    def TOPICS(self):
        return self.datablock_cls.TOPICS

    BUILD_RECORDS_COLUMNS_SHORT = ['stage', 'revision', 'scope', 'alias', 'task_id', 'metric', 'status', 'date', 'timestamp', 'runtime_secs']

    def show_build_records(self, *, stage='ALL', full=False, columns=None, all=False, tail=5):
        """
        All build records for a given Databuilder class, irrespective of alias and revision (see `list()` for more specific).
        'all=True' forces 'tail=None'
        """
        short = not full
        recordspace = self._recordspace_()

        frame = None
        try:
            
            filepaths = [recordspace.join(recordspace.root, filename) for filename in recordspace.list()]
            
            frames = {filepath: pd.read_parquet(filepath, storage_options=recordspace.filesystem.storage_options) for filepath in filepaths}
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
        
        return __frame

    def show_build_record_columns(self, *, full=True, **ignored):
        frame = self.show_build_records(full=full)
        columns = frame.columns
        return columns

    def show_build_record(self, *, record=None, full=False):
        import datablocks
        try:
            records = self.show_build_records(full=full, stage='END')
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

    def show_build_batch_graph(self, *, record=None, batch=0, **kwargs):
        g = self.show_build_graph(record=record, node=(batch,), **kwargs) # argument number `batch` to the outermost Request AND
        return g
    
    def show_build_batch_count(self, *, record=None):
        g = self.show_build_graph(record=record) 
        if g is None:
            nbatches = None
        else:
            nbatches = len(g.args)-1 # number of arguments less one to the outermost Request AND(batch_request[, batch_request, ...], extent_request)
        return nbatches
    
    def show_build_transcript(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        if _record is None:
            summary = None
        else:
            summary = _record['report_transcript']
        return summary

    def show_build_scope(self, *, record=None, **ignored):
        _record = self.show_build_record(record=record, full=True)
        if _record is not None:
            scopestr = _record['scope']
            scope = _eval(scopestr)
        else:
            scope = None
        return scope
    
    def UNSAFE_clear_records(self):
        self._recordspace_().remove()
    
    def UNSAFE_clear_request(self):
        blockscope = self.databuilder._blockscope_(**self.scope)
        #tagblockscope = self.databuilder._tagscope_(**blockscope)
        request = Request(self._UNSAFE_clear_block_, **blockscope)
        return request

    def UNSAFE_clear(self):
        request = self.UNSAFE_clear_request()
        self.UNSAFE_clear_records()
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
        revisionstr = repr(self.databuilder.revision)
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
            record_frame = pd.DataFrame.from_records([_record])
            record_frame.index.name = 'index'
            record_filepath = \
                recordspace.join(recordspace.root, f"alias-{alias}-stage-{lifecycle_stage.name}-task_id-{task_id}-datetime-{datestr}.parquet")
            if self.verbose:
                print(f"DATABOOK LIFECYCLE: {lifecycle_stage.name}: Writing build record at lifecycle_stage {lifecycle_stage.name} to {record_filepath}")
            record_frame.to_parquet(record_filepath, storage_options=recordspace.storage_options)
        return _write_record_lifecycle_callback
    
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

        def __datablock(self, roots, filesystem, scope=None):
            self._datablock = self.datablock_cls(roots, filesystem, scope, **self.datablock_kwargs)
            return self._datablock

        @property
        def __alias_revisionspace(self):
            if self.alias is not None:
                _ = self.anchorspace.subspace(self.alias, f"revision={str(self.revision)}",)
            else:
                _ = self.anchorspace.subspace(f"revision={str(self.revision)}",)
            return _

        def __alias_dataspace__(self, topic, **shard):
            # ignore shard, label by alias, revision, topic only
            if topic is None:
                _ = self.revisionspace
            else:
                _ = self.revisionspace.subspace(topic)
            if self.debug:
                print(f"ALIAS SHARDSPACE: formed for topic {repr(topic)}: {_}")
            return _

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
            dbk = self.datablock(datablock_shardroots, scope=datablock_batchscope, filesystem=self.dataspace.filesystem)
            if self.verbose:
                print(f"DBX: building batch for datablock {dbk} constructed with kwargs {self.datablock_kwargs}")
            dbk.build()
            _ = self.block_extent_pathbook(**tagscope)
            return _

        def __display_batch__(self, tagscope, **batchscope):
            if not hasattr(self.datablock, 'display'):
                raise NotImplementedError(f"Class {self.datablock_clstr} does not implement '.display()'")
            datablock_batchscope = self.datablock_cls.SCOPE(**batchscope)
            datablock_shardroots = self.datablock_batchroots(tagscope, ensure=True)
            #FIX: make a call to display with datablock_batchscope
            _ = self.datablock(datablock_shardroots, filesystem=self.dataspace.filesystem).display
            return _

        def __read_block__(self, tagscope, topic, **blockscope):
            # tagscope can be a list, opaque to the Request evaluation mechanism, but batchscope must be **-expanded to allow Request mechanism to evaluate the kwargs
            datablock_blockroots = self.datablock_blockroots(tagscope)
            datablock_blockscope = self.datablock_cls.SCOPE(**blockscope)
            if topic == None:
                assert not hasattr(self.datablock_cls, 'TOPICS'), f"__read_block__: None topic when datablock.TOPICS == {self.datablock_cls.TOPICS} "
                _ = self.datablock(datablock_blockroots, scope=datablock_blockscope, filesystem=self.dataspace.filesystem).read()
            else:
                _ = self.datablock(datablock_blockroots, scope=datablock_blockscope, filesystem=self.dataspace.filesystem).read(topic)
            return _

        def __shard_extent_pathpage_valid__(self, topic, tagscope, **shardscope):
            datablock_tagshardscope = self.datablock_cls.SCOPE(**shardscope)
            datablock_shardroots = self.datablock_shardroots(tagscope)
            if topic == None:
                assert not hasattr(self.datablock_cls, 'TOPICS'), f"__shard_extent_pathpage_valid__: None topic when datablock_cls.TOPICS == {getattr(self.datablock_cls, 'TOPICS')} "
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem,).valid()
            else:
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem).valid(topic)
            return _
        
        def __extent_shard_metric__(self, topic, tagscope, **shardscope):
            datablock_tagshardscope = self.datablock_cls.SCOPE(**shardscope)
            datablock_shardroots = self.datablock_shardroots(tagscope)
            if topic == None:
                assert not hasattr(self.datablock_cls, 'TOPICS'), f"__extent_shard_metric__: None topic when datablock_cls.TOPICS == {getattr(self.datablock_cls, 'TOPICS')} "
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem).metric()
            else:
                _ = self.datablock(datablock_shardroots, scope=datablock_tagshardscope, filesystem=self.dataspace.filesystem).metric(topic)
            return _

        rangecls = RANGE if not hasattr(dbx.datablock_cls, 'RANGE') else dbx.datablock_cls.RANGE
        SCOPE_fields = dataclasses.fields(dbx.datablock_cls.SCOPE)
        __block_keys = [field.name for field in SCOPE_fields]
        __block_defaults = {field.name: field.default  for field in SCOPE_fields if field.default != dataclasses.MISSING}
        __batch_to_shard_keys = {field.name: field.name for field in SCOPE_fields if isinstance(field.type, rangecls)}

        __module_name = DBX_PREFIX + "." + dbx.datablock_module_name
        __cls_name = dbx.datablock_cls.__name__
        __datablock_cls = dbx.datablock_cls
        __datablock_kwargs = dbx.datablock_kwargs
        if hasattr(dbx.datablock_cls, 'TOPICS'):
            __topics = dbx.datablock_cls.TOPICS
        else:
            __topics = [DEFAULT_TOPIC]

        if hasattr(dbx.datablock_cls, 'REVISION'):
            if dbx.pic:
                __revision = f"{{{dbx.datablock_clstr}.REVISION}}"
            else:
                __revision = dbx.datablock_cls.REVISION
        else:
            __revision = DEFAULT_REVISION
        databuilder_classdict = {
                    '__module__': __module_name,
                    'block_keys': __block_keys, 
                    'block_defaults': __block_defaults,
                    'batch_to_shard_keys': __batch_to_shard_keys,
                    '__init__': __init__,
                    '__repr__': __repr__,
                    'revision': __revision,
                    'topics': __topics,
                    'datablock_cls': __datablock_cls,
                    'datablock_kwargs': __datablock_kwargs,
                    'datablock': __datablock,
                    'datablock_blockroots': __datablock_blockroots,
                    'datablock_batchroots': __datablock_batchroots,
                    'datablock_shardroots': __datablock_shardroots,
                    '_build_batch_': __build_batch__,
                    '_read_block_':  __read_block__,
                    'display_batch': __display_batch__,
        }
        if dbx.alias_dataspace:
            databuilder_classdict['revisionspace'] = __alias_revisionspace
            databuilder_classdict['_shardspace_'] = __alias_dataspace__
        if hasattr(dbx.datablock_cls, 'valid'):
            databuilder_classdict['_shard_extent_pathpage_valid_'] = __shard_extent_pathpage_valid__
        if hasattr(dbx.datablock_cls, 'metric'):
            databuilder_classdict['_extent_shard_metric_'] = __extent_shard_metric__

        databuilder_class = type(__cls_name, 
                               (Databuilder,), 
                               databuilder_classdict,)
        return databuilder_class

    @property
    def tagscope(self):
        _ = self.databuilder._tagscope_(**self.scope)
        return _

    def transcribe(self, with_env=(), with_linenos=False, with_display=False, verbose=False, with_build=False):
        return DBX.Transcribe(self, with_env=with_env, with_linenos=with_linenos, with_display=with_display, with_build=with_build, verbose=verbose,)

    @staticmethod
    def Transcribe(*dbxs, with_env: Tuple[str] = (), with_linenos=False, with_build=False, with_display=tuple(), verbose=False, ):
        """
            Assume dbxs are ordered in the dependency order and all have unique aliases that can be used as variable prefixes.
            TODO: build the dependency graph and reorder, if necessary.
            Examples:
            * Bash definitions (pic dataspace DATABLOCKS_PICLAKE)
            ```
            export MIRCOHN="datablocks.DBX('datablocks.test.micron.datasets.miRCoHN', 'mircohn').Databuilder(dataspace=datablocks.DATABLOCSK_PICLAKE).Datablock(verbose=True)"
            export MIRCOS="datablocks.DBX('datablocks.test.micron.datasets.miRCoStats', 'mircoshn').Databuilder(dataspace=datablocks.DATABLOCKS_PICLAKE).Datablock(verbose=True).SCOPE(mirco=$MIRCOHN.reader('counts'))"
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
            imports[dbx.datablock_module_name]= f"import {dbx.datablock_module_name}\n"
            _datablock = f"{dbx.alias}"
            build += f"# {tag(dbx)}\n"
            blockscope = dbx.databuilder._blockscope_(**dbx.scope)
            tagscope = dbx.databuilder._tagscope_(**blockscope)

            if len(dbx.scope):
                _blockscope  = f"{dbx.datablock_clstr}.SCOPE(\n"
                for key, arg in dbx.scope.items():
                    if isinstance(arg, DBX.Pather):
                        if arg.topic is None:
                            path = f"{arg.dbx.alias}.roots"
                        else:
                            path = f"{arg.dbx.alias}.roots[{repr(arg.topic)}]"
                        _blockscope += f"\t\t{key}={path},\n"
                    elif isinstance(arg, DBX.Reader):
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
                _filesystem = signature.Tagger().tag_func("fsspec.filesystem", protocol, **storage_options) 
            else:
                _filesystem = ""        
            _, __kwargs = Tagger().tag_args_kwargs(**dbx.datablock_kwargs)
            _kwargs = ""
            for key, val in __kwargs.items():
                _kwargs += f"{key}={val},\n"
            
            build += f"{_datablock} = " + Tagger().ctor_name(dbx.datablock_cls) + "(\n"     + \
                            (f"\troots={_blockroots},\n" if len(_blockroots) > 0 else "")      + \
                            (f"\tfilesystem={_filesystem},\n" if len(_filesystem) > 0 else "") + \
                            (f"\tscope={_blockscope},\n" if len(_blockscope) > 0 else "")      + \
                            (f"\t{_kwargs}")                                                + \
                     ")\n"
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
    

PICLAKE = DATABLOCKS_DATALAKE.clone("{DATALAKE}")
def TRANSCRIBE(dbx, piclake=PICLAKE):
    print(dbx.clone(dataspace=piclake, alias_dataspace=True, pic=True, verbose=False).Datablock(verbose=True).transcribe(with_build=True))
            
            

            
    
    