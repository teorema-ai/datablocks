import concurrent.futures
from contextlib import contextmanager
import datetime
import enum
import functools
import json
import logging
import multiprocessing
import os
import pdb
import pickle
import shutil
import sys
import tempfile
import time
import traceback
import uuid

import hashlib

import requests

import google.auth
import google.oauth2.id_token
import google.auth.transport
import google.auth.transport.requests

import pandas as pd

import dask as dd
import dask.distributed
import ray
import ray.util

from .. import signature, utils
from ..dataspace import DATABLOCKS_DATALAKE
from ..signature import tag
from . import request
from .request import Task, Request, Response, Closure
from ..utils import REMOVE, DEPRECATED


VERSION = 0  #TODO: #REMOVE?


class ConstResponse:
    def __init__(self, result, *, exception=None, traceback=None):
        self._result = result
        self._exception = exception
        self._traceback = traceback

    def __str__(self):
        return signature.Tagger().str_ctor(self.__class__,
                                      [self._result], {})
    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__,
                                         [self._result],
                                         dict(
                                            exception=self._exception,
                                            traceback=self._traceback,
                                         )
        )

    def result(self):
        if self._exception is not None:
            traceback = self.traceback()
            if traceback is not None:
                logger.error(f"Error evaluating request:\n{traceback}")
                raise self._exception.with_traceback(traceback)
            else:
                raise self._exception
        return self._result

    def done(self):
        return True

    def running(self):
        return False

    def exception(self):
        return self._exception

    def traceback(self):
        return self._traceback

    def add_done_callback(self, callback):
        self.done_callback = callback
        callback(self)


@contextmanager
def logging_context(logspace, logpath, debug=False):
    if logspace is None or logpath is None:
        yield None
    elif logspace.is_local():
        logfile = open(logpath, 'w', 1)
        if debug:
            print(f"DEBUG: logging_context: opened file {logpath}")
        yield logfile
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpfilepath = os.path.join(tmpdir, 'log')
            tmpfile = open(tmpfilepath, 'w')
            yield tmpfile
            tmpfile.flush()
            logspace.filesystem.put(tmpfilepath, logpath)


class Logging:
    # TODO: guard for datablocks version mismatch
    class Task(Task):
        def __init__(self, pool, func, cookie):
            self._init_((pool, func, cookie, None))

        def _init_(self, state):
            pool, func, cookie, logname = state
            self.pool = pool
            self.func = func
            self.logname = logname
            self.signature = signature.func_signature(self.func)
            self.__defaults__ = signature.func_defaults(self.func)
            self.__kwdefaults__ = signature.func_kwdefaults(self.func)

            # TODO: do we need request? It is not being evaluated directly, but reconstituted using its func,
            # TODO: - which contains all of the request/functor specificity. Input evaluation is being done using
            # TODO: - polymorphism of the *input* requests via their '.evaluate()' methods.
            # TODO: YES! We do need request, since it may need to encapsulate inner functors.
            # TODO: NO! Tasks encapsulate functors
            self.cookie = cookie
            # TODO: deprecate `tag'?
            # TODO: - `id` is used both to create validate tasks and create `logname`
            ## TODO: clarify relationship between id and tag;
            ## TODO: - `id` is used to validate task;
            ## TODO: - unique per request, more precisely, uniquely determined by `key`
            ## TODO: - `tag` seems to be "more unique" -- one per invocation, hence, one per task
            ## TODO: - determines logname together with date 'now' and self.pool.timeus()
            self.id = pool.key_to_id(self.cookie)
            
            if self.pool.log_to_file:
                self.logspace = pool.anchorspace.ensure()
            else:
                self.logspace = None
            # We wait till the end to generate a logname, since it requires self.id
            if self.logname is None:
                now = datetime.datetime.now()
                badge = f"{now.strftime('%Y-%m-%d')}-{self.pool.timeus()}"
                self.logname = f"task-{self.id:028d}-{badge}.log"

        def __repr__(self):
            _ = signature.Tagger().repr_ctor(Logging.Task, [self.pool, self.func, self.cookie])
            return _

        def __setstate__(self, state):
            self._init_(state)

        def __getstate__(self):
            return self.pool, self.func, self.cookie, self.logname

        @DEPRECATED 
        def clone(self):
            # TODO: reuse self.cookie and self.id so that no calls to self.pool.key() etc.
            #  are involved. This way cloning won't require talking to the db.
            clone = self.__class__(self.pool, self.func, self.cookie)
            return clone

        @property
        def logpath(self):
            if self.logname is None or self.logspace is None:
                return None
            logpath = self.logspace.join(self.logspace.path, self.logname)
            return logpath

        def __eq__(self, other):
            return isinstance(other, self.__class__) and \
                   self.pool == other.pool and \
                   self.func == other.func and \
                   self.cookie == other.cookie and \
                   self.id == other.id and \
                   self.logspace == other.logspace and \
                   self.logname == other.logname

        def __call__(self, *args, **kwargs):
            # FIX: ensure authenticate_task arguments are as expected
            self.pool.authenticate_task(self.id,
                                    self.cookie,
                                    )
            if self.pool.debug:
                print(f"DEBUG: Logging.Task: {self.id}: BEGAN executing task\ncookie: {self.cookie}, id:{self.id}\nargs: {args}\nkwargs: {kwargs}")
                print(f"DEBUG: Logging.Task: {self.id}: USING logspace: {self.logspace}, logpath: {self.logpath}")
            logcontext = logging_context(self.logspace, self.logpath, self.pool.debug)
            logstream = logcontext.__enter__()
            #DEBUG
            #pdb.set_trace()
            if logstream is not None:
                if self.pool.log_to_file:
                    if self.pool.verbose:
                        print(f"Redirecting stdout to {self.logpath} in {str(self.logspace)}")
                    _stdout = sys.stdout
                    sys.stdout = logstream
            if self.pool.debug:
                print(f"DEBUG: START: Executing task:\ncookie: {self.cookie}, id: {self.id}")
            try:
                _ = self.func(*args, **kwargs)
            finally:
                if self.pool.debug:
                    print(f"DEBIUG: END: Executing task:\ncookie: {self.cookie}, id: {self.id}")
                if logstream is not None:
                    if self.pool.log_to_file:
                        sys.stdout = _stdout
                        if self.pool.debug:
                            print(f"DEBUG: Restored stdout from {self.logpath} in {self.logspace}")
                    if self.pool.debug:
                        print(f"DEBUG: <<<<<<<<< {self.id}: ENDED executing task\ncookie: {self.cookie}, id:{self.id}\nargs: {args}\nkwargs: {kwargs}")
            return _
        

    class TaskExecutor:
        def __init__(self, *, throw=False):
            self.throw = throw

        def submit(self, request):
            return Request.evaluate(request)

        def restart(self):
            pass

    class Request(Request):
        def __init__(self, pool, cookie, func, *args, **kwargs):
            self.pool = pool
            self.cookie = cookie
            self.func = func
            if isinstance(func, pool.Task):
                task = func
            else:
                task = pool.Task(pool, func, cookie=cookie)
            super().__init__(task, *args, **kwargs)

        def __repr__(self):
            _ = signature.Tagger().repr_ctor(self.__class__, [self.pool, self.cookie, self.func, *self.args], self.kwargs)
            return _

        def __str__(self):
            super_str = super().__str__()
            pool_str = str(self.pool)
            _ = f"{super_str}.apply({pool_str})"
            return _
        
        def __tag__(self):
            super_tag = super().__tag__()
            pool_tag = tag(self.pool)
            _ = f"{super_tag}.apply({pool_tag})"
            return _

        def redefine(self, func, *args, **kwargs):
            request = self.__class__(self.pool, self.cookie, func, *args, **kwargs)\
                    .with_settings(**self.settings)
            return request
        
        def rebind(self, *args, **kwargs):
            request = self.__class__(self.pool, self.cookie, self.task, *args, **kwargs)\
                    .with_settings(**self.settings)
            return request

        def evaluate(self):
            #DEBUG
            #pdb.set_trace()
            r = self.pool.evaluate(self)
            return r

        @property
        def id(self):
            return self.task.id

        @property        
        def key(self):
            return self.task.cookie

        @property
        def logpath(self):
            return self.task.logpath

    class Response(Response):
        def __init__(self,
                     request,
                     *,
                     start_time,
                     pool,
                     done_callback=None,
        ):
            super().__init__(request, start_time=start_time, done_callback=done_callback)
            self.pool = pool

        def __repr__(self):
            return signature.Tagger().repr_ctor(self.__class__,
                                            [self.request,],
                                            dict(
                                                start_time=self.start_time,
                                                pool=self.pool,
                                                done_callback=self._done_callback,
                                            )
            )   

    def __init__(self,
                 name=None,
                 *,
                 dataspace,
                 authenticate_tasks=False,
                 throw=False,
                 log_to_file=False,
                 verbose=False,
                 debug=False,
        ):
        state = dict(name=name,
                dataspace=dataspace,
                authenticate_tasks=authenticate_tasks, 
                throw=throw,
                log_to_file=log_to_file,
                verbose=verbose,
                debug=debug,
        )
        Logging.__setstate__(self, state)

    def __getstate__(self):
        state = dict(name=self.name,\
                    dataspace=self.dataspace,\
                    authenticate_tasks=self.authenticate_tasks, \
                    throw=self.throw, \
                    log_to_file=self.log_to_file,\
                    verbose=self.verbose,
                    debug=self.debug,
        )
        return state

    def __setstate__(self, state):
        self.name, \
        self.dataspace,\
        self.authenticate_tasks, \
        self.throw, \
        self.log_to_file,\
        self.verbose,\
        self.debug,\
            = tuple(state.values())

        self.anchorchain = 'datablocks', 'eval', 'pool', 'Logging'
        if self.name:
            self.anchorchain = self.anchorchain + (self.name,)
        self.anchorspace = self.dataspace.subspace(*self.anchorchain).ensure()

        self._executor = None
        surname = self.__class__.__name__.lower()
        self.logpath = surname if self.name is None else os.path.join(surname, self.name)

        self._ids = None

    def __call__(self, **kwargs):
        return self.clone(**kwargs)

    def clone(self, **kwargs):
        state = self.__getstate__()
        state.update(**kwargs)
        clone = self.__class__(**state)
        return clone

    @property
    def ids(self):
        from ..config import USER, POSTGRES
        from ..hub.db import Ids
        if self._ids is None:
            self._ids = Ids(self.anchorspace,
              user=USER,
              postgres=POSTGRES,
              postgres_schemaname="request",
              postgres_tablename="ids")
        return self._ids

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.__getstate__() == other.__getstate__()

    def __tag__(self):
        if self.name is not None:
            _ = self.name
        else:
            _ = f"{signature.Tagger().ctor_name(self.__class__)}(dataspace={self.dataspace})"
        return _

    def __str__(self):
        s = f"{signature.Tagger().ctor_name(self.__class__)}({repr(self.name)+', ' if self.name else ''}" + \
              f"dataspace={self.dataspace})"
        return s

    #TODO: -> repr_ctor
    def __repr__(self):
        _ = f"{signature.Tagger().ctor_name(self.__class__)}(" +\
               f"{repr(self.name)}, " + \
               f"dataspace={repr(self.dataspace)}, " + \
               f"authenticate_tasks={repr(self.authenticate_tasks)}, " + \
               f"throw={repr(self.throw)}, " + \
               f"log_to_file={repr(self.log_to_file)}, " + \
               ")"
        return _

    def restart(self):
        self.executor.restart()

    def apply(self, request):
        cookie = repr(request)
        #DEBUG
        #pdb.set_trace()
        return self.Request(self, cookie, request.task, *request.args, **request.kwargs).set(throw=self.throw)

    def key_to_id(self, key, *, version=VERSION, unique_hash=True):
        if self.authenticate_tasks:
            key_ = self.ids.sanitize_value(key, quote=False)
            id = self.ids.get_id(key_, version, unique_hash=unique_hash)  # TODO: do we need to have a unique hash?
        else:
            id = utils.key_to_id(key)
        return id

    @staticmethod
    def timeus():
        now = datetime.datetime.now()
        td = pd.Timestamp(now) - pd.Timestamp(0)
        ts = int(td.total_seconds())
        us = td.microseconds
        tus = int(ts * 1e+6 + us)
        return tus

    def authenticate_task(self, id, key):
        if not self.authenticate_tasks:
            return
        from ..config import USER, POSTGRES
        from ..hub.db import Ids
        # REMOVE
        # TODO: use self.ids?
        ids = Ids(self.anchorspace,
                   user=USER,
                   postgres=POSTGRES,
                   postgres_schemaname="request",
                   postgres_tablename="ids")
        dataspace_ = ids.sanitize_value(str(self.anchorspace), quote=False)
        key_ = ids.sanitize_value(key, quote=False)
        _dataspace, _key, _version = ids.lookup_id(id)
        if _dataspace != dataspace_:
            raise ValueError(
                f"Task id {id} pool dataspace mismatch: key: {key}\n\texpected:  {dataspace_}\n\tlooked up: {_dataspace} in ids db {ids}")
        if _key != key_:
            raise ValueError(
                f"Task id {id} normalized key mismatch: \texpected: {key_}, looked up:{_key} in db {self.db}")
        logging.info(
            f"Validated task id {id} from db {self.db}, dataspace: {_dataspace}, key: {_key}, version: {_version}")

    # TODO: move to ctor or REMOVE: use ids instead?
    @property
    def db(self):
        from ..config import POSTGRES_DB
        return POSTGRES_DB

    @property
    def executor(self):
        if self._executor is None:
            self._executor = Logging.TaskExecutor(throw=self.throw)
        return self._executor

    def evaluate(self, request):
        assert isinstance(request, self.__class__.Request)
        #DEBUG
        #pdb.set_trace()
        delayed = self._delay_request(request)
        start_time = datetime.datetime.now()
        future = self._submit_delayed(delayed)
        if self.verbose:
            print(f"Submitted delayed request based on task "
                        f"with id {request.task.id} "
                        f"to evaluate request {request} at {start_time}"
                        f" logging to {request.task.logpath}")
        response = self.Response(pool=self,
                                 request=request,
                                 start_time=start_time,
                                 done_callback=self._execution_done_callback)
        return response

    def as_completed(self, responses):
        for p in responses:
            yield p

    def _submit_delayed(self, delayed):
        future = self.executor.submit(delayed)
        return future

    def _delay_request(self, request):
        _args, _kwargs = self._delay_request_args_kwargs(request)
        delayed = request.rebind(*_args, **_kwargs)
        return delayed

    def _delay_request_args_kwargs(self, request):
        _args = [self._delay_request_arg(request, arg, i) for i, arg in enumerate(request.args)]
        _kwargs = {key: self._delay_request_kwarg(request, arg, key) for key, arg in request.kwargs.items()}
        return _args, _kwargs

    def _delay_request_arg(self, request, arg, index):
        delayed = self._delay_request_argument(request, arg)
        return delayed

    def _delay_request_kwarg(self, request, kwarg, key):
        delayed = self._delay_request_argument(request, kwarg)
        return delayed

    def _delay_request_argument(self, request, arg):
        result = self._delay_request(arg) if isinstance(arg, self.__class__.Request) else arg
        return result

    @staticmethod
    def _execution_done_callback(response):
        pass


class Dask(Logging):
    class Task(Logging.Task):
        pass

    class Request(Logging.Request):
        def __init__(self, pool, request, dask_key=None):
            super().__init__(pool, request)
            self.dask_key = dask_key if dask_key is not None else request.tag

    def __init__(self,
                 name=None,
                 *,
                 dataspace,
                 scheduler_url=None,
                 n_workers=12,
                 authenticate_tasks=False,
                 throw=False,
                 log_to_file=True,
                 verbose=False,
                 debug=False,
        ):
        _kwargs = dict(dataspace=dataspace,
                       authenticate_tasks=authenticate_tasks,
                       throw=throw,
                       log_to_file=log_to_file,
                       verbose=verbose,
                       debug=debug)
        super().__init__(name, **_kwargs)
        if scheduler_url is not None:
            self.scheduler_url = scheduler_url
            self.n_workers = None
        else:
            self.scheduler_url = scheduler_url
            self.n_workers = n_workers

    def __getstate__(self):
        state = super().__getstate__(), self.scheduler_url, self.n_workers
        return state

    def __setstate__(self, state):
        _state, scheduler_url, n_workers = state
        super().__setstate__(_state)
        self.scheduler_url = scheduler_url
        self.n_workers = n_workers

    def apply(self, request, dask_key=None):
        _request = self.Request(self, request, dask_key)
        closure = Closure(_request) # must close, since Dask controls its subgraph
        return closure

    @property
    def executor(self):
        if self._executor is None:
            if self.scheduler_url is not None:
                self._executor = dd.distributed.Client(self.scheduler_url)
            else:
                self._executor = dd.distributed.Client(n_workers=self.n_workers)
        return self._executor

    @property
    def client(self):
        return self.executor

    def restart(self):
        self.executor.restart()
        # TODO: why doesn't the inherited method work?

    def as_completed(self, responses):
        futures = (r.future for r in responses)
        for f in dd.distributed.as_completed(futures):
            yield f.response

    def _submit_delayed(self, delayed):
        future = self.client.compute(delayed)
        return future

    def _delay_request(self, request):
        # NB:
        # This will rewrite args, kwargs and turn any Dask.Request arg into dask.delayed of the underlying arg.task
        # This has the effect of hiding (internalizing in Dask) the (Dask.)Request subgraph attached to this request.
        # This is done to allow Dask to schedule its execution graph statically and to prevent a lock between parent 
        # and child Dask tasks.  For example, if the parent busy-waits on its children, and there is no capacity to run 
        # the parent and all of the children, then a (dead?/live?)lock occurs.  Perhaps this can be remedied by non-busy
        # waiting or dynamic scheduling?  Dynamic scheduling seems to have the same effect as busy waiting
        _delayed_args, _delayed_kwargs = self._delay_request_args_kwargs(request)
        delayed = dask.delayed(request.task)(*_delayed_args, dask_key_name=request.dask_key, **_delayed_kwargs)
        return delayed

    def _delay_request_argument(self, request, arg):
        delayed = self._delay_request(arg) if isinstance(arg, Dask.Request) else arg
        return delayed


class Ray(Logging):
    RAY_SET_TRACE = False # DEBUG
    class Task(Logging.Task):
        def __call__(self, *args, **kwargs):
            if Ray.RAY_SET_TRACE:
                ray.util.pdb.set_trace()
            super().__call__(*args, **kwargs)
            
    class Request(Logging.Request):
        pass

    class Response(Logging.Response):
        def __init__(self, 
                 request,
                 future=None,
                 *,
                 pool=None,
                 start_time = None,
                 done_callback=None
        ):
            self.future = future
            super().__init__(request, pool=pool, start_time=start_time, done_callback=done_callback)

        def __str__(self):
            return signature.Tagger().str_ctor(self.__class__, 
                                               [self.request],
                                               {}, 
            )
    
        def __repr__(self):
            return signature.Tagger().repr_ctor(self.__class__, 
                                            [self.request], 
                                            dict(
                                                start_time=self.start_time,
                                                done_callback=self._done_callback,
                                            )
            )
        
        def _compute(self):
            self._result = self.future.result()
            self._exception = self.future.exception()
            self._traceback = None
            '''
            try:
                self._result = ray.get(self.ref)
            except Exception as e:
                self._exception = e
                self._traceback = e.traceback
            '''

        @property
        def done(self):
            return self.future.done()

        @property
        def running(self):
            return self.future.running()

    def __init__(self,
                 name='Ray',
                 *,
                 dataspace,
                 ray_kwargs=None,
                 ray_working_dir_config={},
                 authenticate_tasks=False,
                 throw=False,
                 log_to_file=False,
                 verbose=False,
                 debug=False,
                ):
        _kwargs = dict(dataspace=dataspace,
                       authenticate_tasks=authenticate_tasks,
                       throw=throw,
                       log_to_file=log_to_file,
                       verbose=verbose,
                       debug=debug,)
        super().__init__(name, **_kwargs)
        self.ray_kwargs = ray_kwargs
        self._ray_client = None
        self.ray_working_dir_config = ray_working_dir_config

    def __str__(self):
        s = f"{signature.Tagger().ctor_name(self.__class__)}({repr(self.name)+', ' if self.name else ''}" + \
              f"dataspace={self.dataspace})"
        return s

    def __repr__(self):
        _ = f"{signature.Tagger.ctor_name(self.__class__)}({repr(self.name)}, " + \
               f"dataspace={self.dataspace}, " + \
               f"ray_kwargs={self.ray_kwargs}, " + \
               f"ray_working_dir_config={self.ray_working_dir_config}, " + \
               f"authenticate_tasks={self.authenticate_tasks}, " + \
               f"throw={self.throw}, " + \
               f"log_to_file={self.log_to_file})"
        return _

    def __delete__(self):
        if self._ray_client is not None:
            self._ray_client.disconnect()
        self._ray_client = None

    def __getstate__(self):
        state = super().__getstate__()
        state.update(**dict(ray_kwargs=self.ray_kwargs,
                            ray_working_dir_config=self.ray_working_dir_config))
        return state

    def __setstate__(self, state):
        _state = {key: val for key, val in state.items() if key not in ['ray_kwargs', 'ray_working_dir_config']}
        self.ray_kwargs = state['ray_kwargs']
        self.ray_working_dir_config = state['ray_working_dir_config']
        super().__setstate__(_state)
        self._ray_client = None

    def repr(self, request):
        pool_key = signature.Tagger(tag_defaults=False) \
            .repr_ctor(self.__class__,
                       [repr(self.name)],
                       dict(
                            dataspace=self.dataspace,
                            ray_kwargs=self.ray_kwargs,
                            ray_working_dir_config=self.ray_working_dir_config
                        )
            )
        key = f"{pool_key}[[{request.tag}]]"
        return key

    @property
    def client(self):
        if self._ray_client is None:
            from ..config import USER
            from .raybuilder import get_ray_client
            
            namespace = f"{USER}.{self.name}"
            self._ray_client = get_ray_client(namespace=namespace,
                                          ray_kwargs=self.ray_kwargs,
                                          ray_working_dir_config=self.ray_working_dir_config,
                                          verbose=self.verbose,)
        return self._ray_client

    @property
    def executor(self):
        return self._ray_client

    def restart(self):
        self.__delete__()
        _ = self.client

    def as_completed(self, responses):
        futures = (r.future for r in responses)
        for f in concurrent.futures.as_completed(futures):
            yield f.response

    def evaluate(self, request):
        assert isinstance(request, self.__class__.Request)
        #task = request.task.clone() # why clone?  That changes task.logname
        task = request.task
        client = self.client
        if self.RAY_SET_TRACE:
                pdb.set_trace()
        if client is None:
            ref = ray.remote(Ray.submit).remote(task, *request.args, **request.kwargs)
            future = ref.future()
        else:
            with self.client:
                ref = ray.remote(Ray.submit).remote(task, *request.args, **request.kwargs)
                #HACK
                time.sleep(2)
                # If future() is called immediately, the future will be returned 
                # in a 'pending' state, will remain in it, and future.result() etc.
                # will hang.
                # TODO: Possible resolution retain ref, instead of future, and extract
                # result, exception, traceback from ref, if it is possible.
                # Note that this needs to be done carefully to ensure compatibility
                # with the parent class.
                future = ref.future()
        start_time = datetime.datetime.now()
        #DEBUG
        #pdb.set_trace()
        if self.verbose:
            print(f"Ray: submitted task "
                        f"with id {task.id} "
                        f"to evaluate request {request} at {start_time}"
                        f" logging to {task.logpath}")
        response = self.Response(request,
                                 future,
                                 pool=self,
                                 start_time=start_time,
                                 done_callback=self._execution_done_callback,
        )
        return response

    @staticmethod
    def submit(task, *args, **kwargs):
        _ = task(*args, **kwargs)
        return _


class Multiprocess(Logging):
    class Task(Logging.Task):
        pass

    class Response(Logging.Response):
        pass

    class Request(Logging.Request):
        pass

    def __init__(self,
                 name='multiprocess',
                 *,
                 dataspace,
                 max_workers=12,
                 authenticate_tasks=False,
                 log_to_file=True,
                 verbose=False,
                 debug=False,):
        _kwargs = dict(dataspace=dataspace,
                         authenticate_tasks=authenticate_tasks,
                         log_to_file=log_to_file,
                         verbose=verbose,
                         debug=debug,)
        super().__init__(name, **_kwargs)
        self.max_workers = max_workers

    def __getstate__(self):
        state = super().__getstate__(), self.max_workers
        return state

    def __setstate__(self, state):
        _state, max_workers = state
        super().__setstate__(_state)
        self.max_workers = max_workers

    @property
    def executor(self):
        if self._executor is None:
            self._executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers,
                                                                    mp_context=multiprocessing.get_context('spawn'))
        return self._executor

    def as_completed(self, responses):
        futures = (p.future for p in responses)
        for f in concurrent.futures.as_completed(futures):
            yield f.response


class HTTP(Logging):
    '''
    class Future(Logging.TaskFuture):
        # HTTP inherits _delay_request() from Logging, whose 'delayed' is a Task  returning Report
        # so we need this Future to be able to unpack it.
        def __init__(self, result, *, exception=None, traceback=None):
            super().__init__(result, exception=exception, traceback=traceback)
    '''
    class Response(Logging.Response):
        pass

    class Executor:
        """Logically this is part of HTTP pool. Factored out to fit into the Local framework."""
        def __init__(self, pool):
            self.pool = pool

        def add_auth_header(self, auth, headers):
            if auth is None:
                return headers
            if 'type' not in auth or auth['type'] not in ['google']:
                raise ValueError(f"Uknown type of auth: {auth}")
            if 'service_account_info' in auth and auth['service_account_info'] is not None:
                service_account_info = auth['service_account_info']
                with tempfile.TemporaryDirectory() as tempdir:
                    filename = os.path.join(tempdir, 'service_account_credentials.json')
                    json.dump(service_account_info, open(filename, 'w'))
                    assert service_account_info == json.load(open(filename))
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = filename
                    google.auth.default()
                    auth_req = google.auth.transport.requests.Request()
                    target_audience = self.pool.url
                    id_token = google.oauth2.id_token.fetch_id_token(auth_req, target_audience)
                    headers.update({"Authorization": f"Bearer {id_token}"})
                if self.verbose:
                    print(f"Added authorization to headers {headers} for pool url {self.pool.url}")
            return headers

        def submit(self, request):
            result = None
            exception = None
            traceback = None
            request_pickle = pickle.dumps(request)
            if self.pool.url is not None:
                headers = self.add_auth_header(self.pool.auth, {})
                http_response = requests.post(self.pool.url+'/eval', headers=headers, data=request_pickle)
                if self.debug:
                    print(f"DEBUG: Response status_code: {http_response.status_code}")
                response_pickle = http_response.content
            else:
                #FIX: serve
                response_pickle = serve._eval(request_pickle, throw=self.pool.throw)
            response = pickle.loads(response_pickle)
            if 'result' in response:
                # Assume that result is a Report, just like in evaluating a Task
                result = pickle.loads(response['result'])
            if 'exception' in response:
                exception = pickle.loads(response['exception'])
                if self.debug:
                    print(f"DEBUG: Response exception: {exception}")
            if 'traceback' in response:
                traceback = pickle.loads(response['traceback'])
                if self.debug:
                    print(f"DEBUG: Response traceback: {traceback}")
            future = HTTP.Future(result, exception=exception, traceback=traceback)
            return future

    def __init__(self,
                 name='http',
                 *,
                 dataspace,
                 url=None,
                 auth=None,
                 authenticate_tasks=False,
                 throw=False,
                 log_to_file=True,
                 verbose=False,
                 debug=False,):
        _state = name, \
                dataspace, \
                authenticate_tasks, \
                throw, \
                log_to_file, \
                verbose, \
                debug,\
        state = _state, url, auth
        self.__setstate__(state)

    def __getstate__(self):
        _state = super().__getstate__()
        state = _state, self.url, self.auth
        return state

    def __setstate__(self, state):
        _state, self.url, self.auth = state
        super().__setstate__(_state)
        self.anchorspace = self.dataspace.subspace('datablocks', 'eval', 'pool', 'HTTP', self.name).ensure()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
               super().__eq__(other) and \
               self.url == other.url and \
               self.auth == other.auth

    def __repr__(self):
        _ = f"{signature.Tagger.ctor_name(self.__class__)}({repr(self.name)}, " + \
               f"dataspace = {self.dataspace}, " + \
               f"url = {self.url}, " + \
               f"auth = {self.auth}, " + \
               f"authenticate_tasks = {self.authenticate_tasks}, " + \
               f"throw = {self.throw}, " + \
               f"log_to_file = {self.log_to_file})"
        return _

    def repr(self, request):
        pool_key = signature.Tagger(tag_defaults=False) \
            .repr_ctor(self.__class__, [repr(self.name)], dict(dataspace=self.dataspace, url=self.url))
        key = f"{pool_key}[[{request.tag}]]"
        return key

    @property
    def executor(self):
        if self._executor is None:
            self._executor = self.Executor(self)
        return self._executor

    def _delay_request(self, request):
        delayed = request.request
        return delayed


def print_all_responses(tasks):
    import time
    print(len(tasks))
    for i, t in enumerate(tasks):
        print(f"{i}: {t.tag}: {t}: start_time: {t.start_time}")
        time.sleep(0.03)


def done_running_failed_pending_responses(tasks, include_failed=True, *, delay_secs=0.0, details=False):
    import time
    done = {i: t for i, t in enumerate(tasks) if t.done and t.exception() is None}
    print(f"Done: {len(done)}")
    if details:
        for i, t in done.items():
            run_time = t.done_time - t.start_time if t.done_time is not None else 0
            print(f"{i}: {t.tag}: {t}: start_time: {t.start_time}, done_time: {t.done_time}, run_time: {run_time}")
            time.sleep(delay_secs)
    print("********")
    running = {i: t for i, t in enumerate(tasks) if t.running}
    print(f"Running: {len(running)}")
    failed = {i: t for i, t in enumerate(tasks) if t.done and t.exception() is not None}
    if details:
        for i, t in running.items():
            print(f"{i}: {t.tag}:  {t}: start_time: {t.start_time}")
            time.sleep(delay_secs)
    if include_failed:
        print("********")
        print(f"Failed: {len(failed)}")
        if details:
            for i, t in failed.items():
                print(f"{i}: {t.tag}: {t}")
                time.sleep(delay_secs)
    pending = {i: t for i, t in enumerate(tasks) if i not in done and i not in running and i not in failed}
    print("********")
    print(f"Pending: {len(pending)}")
    if details:
        for i, t in pending.items():
            print(f"{i}: {t.tag}: {t}")
            time.sleep(delay_secs)
    return done, running, failed, pending


def iresponse_results(itasks):
    print(f"{len(itasks)}")
    for i, t in itasks.items():
        run_time = t.done_time - t.start_time if t.done_time is not None else 0
        tag = repr(t.request)
        print(f"{i}: {tag}: {t.result()}: run_time: {run_time}")


def iresponse_logs(itasks):
    import time
    print(f"{len(itasks)}")
    for i, t in itasks.items():
        print(f"{i}: {repr(t.request)}: {t}")
        print(t.log(position=0, size=10000000))
        print("****************")
        time.sleep(0.1)


DATABLOCKS_STDOUT_LOGGING_POOL = Logging(name='DATABLOCKS_STDOUT_LOGGING_POOL', dataspace=DATABLOCKS_DATALAKE)
DATABLOCKS_FILE_LOGGING_POOL = Logging(name='DATABLOCKS_FILE_LOGGING_POOL', dataspace=DATABLOCKS_DATALAKE, log_to_file=True)

DATABLOCKS_STDOUT_RAY_POOL = Ray(name='DATABLOCKS_STDOUT_RAY_POOL', dataspace=DATABLOCKS_DATALAKE)
DATABLOCKS_FILE_RAY_POOL = Ray(name='DATABLOCKS_FILE_RAY_POOL', dataspace=DATABLOCKS_DATALAKE, log_to_file=True)

