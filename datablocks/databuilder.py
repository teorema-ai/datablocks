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
import pdb #DEBUG
from typing import Any, TypeVar, Generic, Tuple, Union, List, Dict

import fsspec
import git


import pyarrow.parquet as pq
import pandas as pd


from . import signature
from .signature import Signature, ctor_name
from .signature import tag, Tagger
from .utils import ALIAS, DEPRECATED, OVERRIDE, microseconds_since_epoch, datetime_to_microsecond_str
from .eval import request, pool
from .eval.request import Request, ALL, LAST, NONE, Graph
from .eval.pool import DATABLOCKS_STDOUT_LOGGING_POOL as STDOUT_POOL, DATABLOCKS_FILE_LOGGING_POOL as FILE_POOL
from .eval.pool import DATABLOCKS_STDOUT_LOGGING_POOL as STDOUT_LOGGING_POOL, DATABLOCKS_FILE_LOGGING_POOL as FILE_LOGGING_POOL
from .dataspace import DATABLOCKS_DATALAKE as DATALAKE


_print = __builtins__['print']


logger = logging.getLogger(__name__)


HOME = os.environ['HOME']

T = TypeVar('T')
class RANGE(Generic[T], tuple):
    def __init__(self, *args):
        tuple.__init__(args)
        
    
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
        """
            Converts scope values to their tags to avoid evaluating them.
            For example, when the value is a request to read the result of another
            Databuilder, returning a pd.DataFrame.
        """
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
                 dataspace=DATALAKE,
                 tmpspace=None,
                 lock_pages=False,
                 rebuild=False,
                 pool=STDOUT_POOL,
                 build_block_request_lifecycle_callback=None,
                 throw=None,
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
        self.rebuild = rebuild
        self.pool = pool.clone(throw=throw) if pool is not None else pool
        self.build_block_request_lifecycle_callback = build_block_request_lifecycle_callback
        self.throw = throw
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
            repr = Tagger().tag_ctor(self.__class__, args, kwargs)
            return repr
        else:
            repr = tag.Tagger().tag_ctor(self.__class__, [], {})

    @property
    def revisionspace(self):
        return self.anchorspace.subspace(f"revision={str(self.revision)}",)

    @property
    def tmpspace(self):
        if self._tmpspace is None:
            self._tmpspace = self.revisionspace.temporary(self.revisionspace.subspace('tmp').ensure().root)
        return self._tmpspace

    def block_intent_page(self, topic, **scope):
        blockscope = self._blockscope_(**scope)
        tagblockscope = self._tagscope_(**blockscope)
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        tagshard_list = self.scope_to_shards(**tagblockscope)
        kvhandle_pathshard_list = []
        for tagshard in tagshard_list:
            kvhandle = self._scope_to_kvhandle_(topic, **tagshard)
            pathshard = self._shardspace_(topic, **tagshard).root
            kvhandle_pathshard_list.append((kvhandle, pathshard))
        block_intent_page = {kvhandle: pathshard for kvhandle, pathshard in kvhandle_pathshard_list}
        return block_intent_page

    def block_extent_page(self, topic, **scope):
        if topic not in self.topics:
            raise ValueError(f"Unknown topic {topic} is not among {self.topics}")
        block_intent_page = self.block_intent_page(topic, **scope)
        block_extent_page = {}
        for kvhandle, shard_pathset in block_intent_page.items():
            shardscope = self._kvhandle_to_scope_(topic, kvhandle)
            tagscope = self._tagscope_(**shardscope)
            valid = self._shard_extent_page_valid_(topic, tagscope, **shardscope)
            if valid:
                block_extent_page[kvhandle] = shard_pathset
        return block_extent_page

    @OVERRIDE
    def _shard_extent_page_valid_(self, topic, tagscope, **shardscope):
        pathset = self._shardspace_(topic, **tagscope).root
        valid = self.revisionspace.filesystem.isdir(pathset)
        if self.debug:
            if valid:
                print(f"_shard_extent_page_valid_: VALID: shard with topic {repr(topic)} with scope with tag {tagscope}")
            else:
                print(f"_shard_extent_page_valid_: INVALID shard with topic {repr(topic)} with scope with tag {tagscope}")
        return valid

    #TODO: #REMOVE? Apply self.pool?
    def block_extent_page_request(self, topic, **scope):
        _ = Request(self.block_extent_page, topic, **scope)
        if self.throw is not None:
            _ = _.set(throw=self.throw)
        return _

    def block_shortfall_page(self, topic, **scope):
        block_intent_page = self.block_intent_page(topic, **scope)
        block_extent_page = self.block_extent_page(topic, **scope)
        block_shortfall_page = {}
        for kvhandle, intent_pathshard in block_intent_page.items():
            if isinstance(intent_pathshard, str):
                if kvhandle not in block_extent_page or block_extent_page[kvhandle] != intent_pathshard:
                    shortfall_pathshard = intent_pathshard
                else:
                    shortfall_pathshard = []
            else:
                if kvhandle not in block_extent_page:
                    shortfall_pathshard = intent_pathshard
                else:
                    extent_pathshard = block_extent_page[kvhandle]
                    shortfall_pathshard = [intent_filepath for intent_filepath in intent_pathshard
                                             if intent_filepath not in extent_pathshard]
            if len(shortfall_pathshard) == 0:
                continue
            block_shortfall_page[kvhandle] = shortfall_pathshard
        return block_shortfall_page

    def block_intent_book(self, **scope):
        return {topic: self.block_intent_page(topic, **scope) for topic in self.topics}
        #return self._page_book("intent", **scope) #REMOVE

    def block_extent_book(self, **scope):
        return {topic: self.block_extent_page(topic, **scope) for topic in self.topics}
        #return self._page_book("extent", **scope) #REMOVE

    @staticmethod
    def collate_pages(topic, *pages):
        # Each page is a dict {kvhandle -> filepathset}.
        # `collate` is just a union of dicts.
        # Assuming each kvhandle exists only once in pages or only the last occurrence matters.
        collated_page = {kvhandle: filepathset for page in pages for kvhandle, filepathset in page.items()}
        return collated_page

    @staticmethod
    def collate_books(*books):
        # Collate all pages from all books within a topic
        topic_pages = {}
        for book in books:
            for topic, page in book.items():
                if topic in topic_pages:
                    topic_pages[topic].append(page)
                else:
                    topic_pages[topic] = [page]
        collated_book = \
            {topic: Databuilder.collate_pages(topic, *pages) \
             for topic, pages in topic_pages.items()}
        return collated_book

    def block_intent(self, **scope):
        block_intent_book = self.block_intent_book(**scope)
        block_intent_scopebook = self._kvhbook_to_scopebook(block_intent_book)
        return block_intent_scopebook

    def block_extent(self, **scope):
        #DEBUG
        #blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        #block_extent_book = self.block_extent_book(**tagscope)
        block_extent_book = self.block_extent_book(**scope)
        block_extent_scopebook = self._kvhbook_to_scopebook(block_extent_book)
        return block_extent_scopebook

    def block_shortfall(self, **scope):
        blockscope = self._blockscope_(**scope)
        #tagscope = self._tagscope_(**blockscope)
        block_shortfall_book = self.block_shortfall_book(**blockscope)
        _block_shortfall_book = self._kvhbook_to_scopebook(block_shortfall_book)
        return _block_shortfall_book

    #RENAME: -> _shard_extent_metric_
    def _extent_shard_metric_(self, topic, tagscope, **shardscope):
        block_extent_page = self.block_extent_page(topic, **shardscope)
        if self.debug:
            print(f"_extent_shard_metric_: topic: {repr(topic)}: shardspace: {shardscope}: block_extent_page: {block_extent_page}")
        if len(block_extent_page) > 1:
            raise ValueError(f"Too many shards in block_extent_page: {block_extent_page}")
        if len(block_extent_page) == 0:
            metric = 0
        else:
            pathset = list(block_extent_page.values())[0]
            if isinstance(pathset, str):
                metric = 1
            else:
                metric = len(pathset)
        return metric
    
    def block_extent_metric(self, **scope):
        blockscope = self._blockscope_(**scope)
        extent = self.block_extent(**blockscope)
        _extent_metric_book = {}
        for topic, page in extent.items():
            metric_page = []
            for shardscope, _ in page:
                tagscope = self._tagscope_(**shardscope)
                shardmetric = self._extent_shard_metric_(topic, tagscope, **shardscope)
                #FIX: refactor via *_to_scopebook()
                metric_page.append((shardscope, shardmetric))
            _extent_metric_book[topic] = Databuilder.Scopepage(metric_page)
        extent_metric_book = Databuilder.Scopebook(_extent_metric_book)
        return extent_metric_book

    def block_shortfall_book(self, **scope):
        blockscope = self._blockscope_(**scope)
        pagekvhandles_list = [set(self.block_shortfall_page(topic, **blockscope).keys()) for topic in self.topics]
        bookkvhandleset = set().union(*pagekvhandles_list)
        block_shortfall_book = {}
        for topic in self.topics:
            block_shortfall_page = {}
            for kvhandle in bookkvhandleset:
                scope = {key: val for key, val in kvhandle}
                filepathset = self._shardspace_(topic, **blockscope).root
                block_shortfall_page[kvhandle] = filepathset
            block_shortfall_book[topic] = block_shortfall_page
        return block_shortfall_book
    
    @OVERRIDE
    def _shardspace_(self, topic, **shard):
        #TODO: ensure topic=None is handled correctly: `topic == None` must only be allowed when `self.topics == None` 
        #TODO: disallow the current default `self.topics == [None]` -> `self.topic = None`
        #TODO: when `self.topics is not None` it must be a list of valid str
        #TODO: `self.topics == None` must mean that `_shardspace_` generates a unique space corresponding to an `hvhandle` with no topic head
        #TODO: `scope_to_hvhandle` with topic==None must generate an hvhandle with no topic head
        tagshard = self._tagscope_(**shard) #TODO: #REMOVE?: redundant, since the input shard is part of a tagscope
        hvhandle = self._scope_to_hvhandle_(topic, **tagshard)
        subspace = self.revisionspace.subspace(*hvhandle)
        if self.debug:
            print(f"SHARDSPACE: formed for topic {repr(topic)} and shard with tag {tagshard}: {subspace}")
        return subspace
    
    class Scopebook(dict):
        def pretty(self):
            lines = []
            for topic, page in self.items():
                if topic is None:
                    lines.append(page.pretty())
                else:
                    lines.append(repr(topic) + ":\n" + page.pretty())
            _ = '\n '.join(lines)
            return _
        
    class Scopepage(list):
        def pretty(self):
            lines = []
            for scope, val in self:
                if len(scope) == 0:
                    lines.append(repr(val))
                else:
                    lines.append(scope.pretty() + ": " + repr(val))
            if len(lines) == 0:
                _ = "  [\n   ]"
            else:
                _ = "  [\n\t" + '\n\t'.join(lines) + "\n   ]"
            return _

    class _scope_(dict):
        def pretty(self):
            lines = []
            for key, val in self.items():
                lines.append(f"{repr(key)}: {repr(val)}")
            _ = "\t{\n\t" + '\n\t'.join(lines) + "\n\t}"
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
    
    def _kvhbook_to_scopebook(self, kvhbook):
        _scopebook = {}
        for topic, kvhpage in kvhbook.items():
            if not topic in _scopebook:
                _scopebook[topic] = []
            for kvhandle, val in kvhpage.items():
                scope = Databuilder._scope_(self._kvhandle_to_scope_(topic, kvhandle))
                _scopebook[topic].append((Databuilder._scope_(scope), val))
        scopebook = Databuilder.Scopebook({topic: Databuilder.Scopepage(page) for topic, page in _scopebook.items()}) 
        return scopebook

    def _lock_kvhandle(self, topic, kvhandle):
        if self.lock_pages:
            hivechain = self._kvhandle_to_hvhandle_(topic, kvhandle)
            self.revisionspace.subspace(*hivechain).acquire()

    def _unlock_kvhandle(self, topic, kvhandle):
        if self.lock_pages:
            hivechain = self._kvhandle_to_hvhandle_(topic, kvhandle)
            self.revisionspace.subspace(*hivechain).release()

    #REMOVE: unroll inplace where necessary
    '''
    def _page_book(self, domain, **kwargs):
        # This book is computed by computing a page for each topic separately, 
        # via a dedicated function call with topic as an arg, using a domain-specific
        # method.  domain: 'intent'|'extent'|'shortfall'
        _page = getattr(self, f"block_{domain}_page")
        book = {topic: _page(topic, **kwargs) for topic in self.topics}
        return book
    '''

    @OVERRIDE
    def build_block_request(self, **scope):
        blockscope = self._blockscope_(**scope)
        if self.rebuild:
            block_shortfall_book = self.block_intent_book(**blockscope)
        else:
            block_shortfall_book = self.block_shortfall_book(**blockscope)
        block_shortfall_book_kvhandles_lists = [list(block_shortfall_book[topic].keys()) for topic in self.topics]
        block_shortfall_book_kvhandles_list = [_ for __ in block_shortfall_book_kvhandles_lists for _ in __]
        block_shortfall_book_kvhandles = list(set(block_shortfall_book_kvhandles_list))

        shortfall_batchscope_list = \
            self._kvhandles_to_batches_(*block_shortfall_book_kvhandles)
        shortfall_batchscope_list = \
            [{k: blockscope[k] for k in tscope.keys()} for tscope in shortfall_batchscope_list]
        if self.verbose:
            if len(shortfall_batchscope_list) == 0:
                print(f"Databuilder: build_block_request: no shortfalls found: returning extent")
            else:
                print(f"Databuilder: build_block_request: requesting build of shortfall batchscopes with tags: {shortfall_batchscope_list}")
        _shortfall_batch_requests = \
            [self._build_batch_request_(self._tagscope_(**shortfall_batchscope_list[i]), **shortfall_batchscope_list[i])
                            for i in range(len(shortfall_batchscope_list))]
        #DEBUG
        #pdb.set_trace()
        shortfall_batch_requests_ = [_.set(throw=self.throw).apply(self.pool) for _ in _shortfall_batch_requests]
        '''
        #TODO: #REMOVE?
        if self.throw is not None:
            shortfall_batch_requests = [_.set(throw=self.throw) for _ in shortfall_batch_requests_]
        else:
            shortfall_batch_requests = shortfall_batch_requests_
        '''
        shortfall_batch_requests = shortfall_batch_requests_

        shortfall_batch_requests_tags = "[" + \
                                          ", ".join(tag(_) for _ in shortfall_batch_requests) + \
                                          "]"
        if self.verbose:
            print(f"build_block_request: shortfall_batch_requests: " + shortfall_batch_requests_tags)

        tagscope = self._tagscope_(**blockscope)
        extent_request = Request(self.block_extent, **tagscope)
        requests = shortfall_batch_requests + [extent_request.set(throw=self.throw)]
        build_block_request = Request(ALL, *requests).set(throw=self.throw)
        '''
        #TODO: #REMOVE: #WARNING: 
        if self.throw is not None:
            build_block_request = build_block_request.set(throw=throw)
        '''
        #TODO: #FIX
        #build_block_request = LAST(*shortfall_batch_requests) if len(shortfall_batch_requests) > 0 else NONE()
        if len(shortfall_batchscope_list) > 0 and self.build_block_request_lifecycle_callback is not None:
            _ = build_block_request.with_lifecycle_callback(self.build_block_request_lifecycle_callback(**blockscope))
            if self.verbose:
                print(f"Databuilder: build_block_request: will record lifecycle")
        else:
            _ = build_block_request
            if self.verbose:
                print(f"Databuilder: build_block_request: will NOT record lifecycle")
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
    def read_block_request(self, topic, **blockscope):
        _blockscope = self._blockscope_(**blockscope)
        _tagscope = self._tagscope_(**_blockscope)
        request = Request(self._read_block_, _tagscope, topic, **blockscope)
        return request
    
   
