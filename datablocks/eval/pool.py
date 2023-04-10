import concurrent.futures
from contextlib import contextmanager
import datetime
import enum
import functools
import json
import logging
import multiprocessing
import os
import pickle
import shutil
import sys
import tempfile
import traceback
import uuid

import hashlib

import requests

import google.auth
import google.oauth2.id_token
import google.auth.transport
import google.auth.transport.requests

import pandas as pd

#import dask as dd
#import dask.distributed
import ray
import ray.util

from .. import signature as tag
from ..dataspace import DATABLOCKS_DATALAKE
from .request import Task, Request, Response


VERSION = 0
logger = logging.getLogger(__name__)



# TODO: Move _delay* functionality into pool's evaluate()

# TODO: Control plane.
# TODO: The "local" part of the 'control plane' is the logspace:Dataspace, containing
# TODO: - per task log files, which can be queried for their existence, assertaining
# TODO: - whether the task ran, and through its contents to assertain success and debug failures.
# TODO: The local part is persistent, but slow and is 'opaque' -- high entropy conditional
# TODO: - on the top/driver task.
# TODO: The "global" part of the 'control plane' connects tasks to their inputs,
# TODO: - is contained in Response objects, which in turn contain Requests (mapping to its inputs)
# TODO: - and delayeds, mapping to tasks, tying into the local control plane via task log file names.
# TODO: The "global" transient control plane is an index into the local control plane, recursively
# TODO: - mapping the driver task to its inputs and their status.
#
# TODO: The nomenclature here may be reversed.  Dataspace is in a sense a 'global' store, containing
# TODO: - statuses of *all* tasks, and accessible from anywhere.
# TODO: - at the same time, transient 'Response' objects are localized in the object/worker running the current
# TODO: - driver task and mapping out only the part of the call history originating from the current (local) task/node.

class ConstFuture:
    def __init__(self, result, *, exception=None, traceback=None):
        self._result = result
        self._exception = exception
        self._traceback = traceback

    def __str__(self):
        return tag.Tagger.str_ctor(self.__class__,
                                      self._result)
    def __repr__(self):
        return tag.Tagger().repr_ctor(self.__class__,
                                         self._result,
                                         exception=self._exception,
                                         traceback=self._traceback)

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
def logging_context(logspace, logpath):
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            if logspace is not None:
                tmpfilepath = os.path.join(tmpdir, 'log')
                tmpfile = open(tmpfilepath, 'w')
                yield tmpfile
            else:
                yield None
        finally:
            if logspace is not None:
                tmpfile.flush()
                logspace.filesystem.put(tmpfilepath, logpath)


class Logging:
    # TODO: guard for datablocks version mismatch
    class Task(Task):
        def __init__(self, pool, request):
            self._init_((pool, request, None))

        # TODO: remove ray_set_trace (or move to Ray.Task?)
        def _init_(self, state, *, ray_set_trace=False):
            if ray_set_trace:
                ray.util.pdb.set_trace()
            pool, request, logname = state
            self.pool = pool
            self.request = request
            self.logname = logname
            self.func = request.func
            self.signature = tag.func_signature(self.func)
            self.__defaults__ = tag.func_defaults(self.func)
            self.__kwdefaults__ = tag.func_kwdefaults(self.func)

            # TODO: do we need request? It is not being evaluated directly, but reconstituted using its func,
            # TODO: - which contains all of the request/functor specificity. Input evaluation is being done using
            # TODO: - polymorphism of the *input* requests via their '.evaluate()' methods.
            # TODO: YES! We do need request, since it may need to encapsulate inner functors.
            self.key = pool.task_key(request)
            # TODO: deprecate `tag'?
            # TODO: - `id` is used both to create validate tasks and create `logname`
            ## TODO: clarify relationship between id and tag;
            ## TODO: - `id` is used to validate task;
            ## TODO: - unique per request, more precisely, uniquely determined by `key`
            ## TODO: - `tag` seems to be "more unique" -- one per invocation, hence, one per task
            ## TODO: - determines logname together with date 'now' and self.pool.timeus()
            self.id = pool.task_id(self.key)
            self.request_tag = tag.tag(request)# TODO: --> str(request)?
            self.request_repr = repr(request) # needed for logging when request itself might not be available (but why?)
            self.__iargs_kargs_kwargs__ = request.iargs_kargs_kwargs()
            if self.pool.log_to_file:
                self.logspace = pool.anchorspace.ensure()
            else:
                self.logspace = None
            # We wait till the end to generate a logname, since it requires self.id
            if self.logname is None:
                now = datetime.datetime.now()
                badge = f"{now.strftime('%Y-%m-%d')}-{self.pool.timeus()}"
                self.logname = f"task-{badge}-{self.id:042d}.log"

        def __repr__(self):
            _ = tag.Tagger().repr_ctor(self.__class__, self.pool, self.request)
            return _

        def __setstate__(self, state):
            self._init_(state, ray_set_trace=False)

        def __getstate__(self):
            return self.pool, self.request, self.logname

        def __delete__(self):
            if self.request.lifecycle_callback is not None and hasattr(self, '_request'):
                self.request.lifecycle_callback(Task.Lifecycle.ERROR, self._request, None)

        def clone(self):
            # TODO: reuse self.key and self.id so that no calls to self.pool.key() etc.
            #  are involved. This way cloning won't require talking to the db.
            clone = self.__class__(self.pool, self.request)
            return clone

        @property
        def logpath(self):
            if self.logname is None:
                return None
            logpath = self.logspace.join(self.logspace.path, self.logname)
            return logpath

        def __eq__(self, other):
            return isinstance(other, self.__class__) and \
                   self.pool == other.pool and \
                   self.func == other.func and \
                   self.key == other.key and \
                   self.id == other.id and \
                   self.request_tag == other.request_tag and \
                   self.request_repr == other.request_repr and \
                   self.logspace == other.logspace and \
                   self.logname == other.logname

        def __call__(self, *args, **kwargs):
            """
            Rebinds must be allowed upon entry because the pool graph may (re)evaluate args/kwargs at the last minute.
            FIX: reconcile logger vs _logging_; reconcile logging and _logging_
            """
            #_logging_ = logging.getLogger(__name__)
            #_logging_ = logging.getLogger()
            _logging_ = logging
            request = self.request.rebind(*args, **kwargs)
            self._request = request
            # FIX: ensure validate_task arguments are as expected
            self.pool.validate_task(self.id,
                                    self.key,
                                    self.request_tag,
                                    self.request_repr)
            request_logger = None
            request_str = str(request)
            _logging_.debug(f">>>>>>>> {self.id}: BEGAN executing request called {request_str}")

            #logger = logging.getLogger(log_file)
            # cannot add handler to a named logger since functions
            # deeper in the call tree will not know to use this logger to write to:
            # they will use logging.getLogger() at best.
            # Thus, we need to add handler to the root logger.
            # N.B.: `logging` does not seem to allow to add handlers to it.
            logger = logging.getLogger()

            _log_level = logger.getEffectiveLevel()
            logger.setLevel(self.pool.log_level)
            logcontext = logging_context(self.logspace, self.logpath)
            logstream = logcontext.__enter__()
            if logstream is not None:
                if self.pool.redirect_stdout:
                    _logging_.debug(f"Redirecting stdout to {self.logpath} in {str(self.logspace)}")
                    _stdout = sys.stdout
                    sys.stdout = logstream
                    _logging_.debug(f"Redirected stdout to {self.logpath} in {str(self.logspace)}")
                else:
                    _logging_.debug(f"Adding logger handler recording to {self.logpath} in {str(self.logspace)}")
                    _handlers = logger.handlers
                    logger.handers = []
                    handler = logging.StreamHandler(logstream)
                    formatter = logging.Formatter(
                        f"{self.pool.log_prefix if self.pool.log_prefix is not None else ''}" + (
                            "" if self.pool.log_prefix is None else ":") +
                        f"{self.pool.log_format}")
                    handler.setFormatter(formatter)
                    if hasattr(request.func, '__globals__') and 'logger' in request.func.__globals__:
                        request_logger = request.func.__globals__['logger']
                        request.func.__globals__['logger'] = logger
                    logger.addHandler(handler)
            logger.debug(f"START: Executing request called {request_str} with task id {self.id}")
            exc = None
            try:
                if self.request.lifecycle_callback is not None:
                    self.request.lifecycle_callback(Task.Lifecycle.BEGIN, request, None)
                response = request.with_throw(self.pool.throw).evaluate(task_key=self.key,
                                                                        task_id=self.id,
                                                                        task_logspace=self.logspace,
                                                                        task_logname=self.logname)
                if self.request.lifecycle_callback is not None:
                    self.request.lifecycle_callback(Task.Lifecycle.END, request, response)
                #_ = response.result()     # TODO: REMOVE?
                #__ = response.exception() # TODO?: REMOVE?
                report = response.report()
                # HACK: need to pass logspace and logname to request.evaluate()
                report.logspace = self.logspace
                report.logspace_url = self.logspace.url if self.logspace is not None else None
                report.logpath = self.logpath
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if self.pool.throw:
                    raise exc_value.with_traceback(exc_traceback)
                tb_lines = traceback.format_tb(exc_traceback)
                tb = '\n'.join(tb_lines)
                logger.error(f"\n{tb}{exc_type}: {exc_value}")
                exc = exc_value
            finally:
                logger.debug(f"STOP: Executing request called {request_str} with task id {self.id}")
                if logstream is not None:
                    if self.pool.redirect_stdout:
                        _logging_.debug(f"Restoring stdout from {self.logpath} in {self.logspace}")
                        sys.stdout = _stdout
                        _logging_.debug(f"Restored stdout from {self.logpath} in {self.logspace}")
                    else:
                        if hasattr(request.func, '__globals__'):
                            request.func.__globals__['logger'] = request_logger
                        logger.handlers = []
                        for h in _handlers:
                            logger.addHandler(h)
                        logcontext.__exit__(None, None, None)
                        _logging_.debug(f"Removed logger handler recording to {self.logpath} in {self.logspace}")
                    logger.setLevel(_log_level)
                    _logging_.debug(f"<<<<<<<< {self.id}: ENDED executing request called {request_str}")
            if exc is not None:
                raise exc
            return report

    class TaskReportFuture(ConstFuture):
        def __init__(self, report, *, exception=None, traceback=None):
            super().__init__(report, exception=exception, traceback=traceback)
            self.report = report

        def __repr__(self):
            return tag.Tagger().repr_ctor(self.__class__,
                                             self.report,
                                             exception=self._exception,
                                             traceback=self._traceback)

        def report(self):
            return self.report

        def result(self):
            exception = self.exception()
            if exception is not None:
                traceback = self.traceback()
                if traceback is not None:
                    logger.error(f"Error evaluating request:\n{traceback}")
                    raise exception.with_traceback(traceback)
                else:
                    raise exception
            return self.report.result

        def exception(self):
            exception = super().exception()
            if exception is None:
                exception = self.report.exception
            return exception

        def traceback(self):
            exception = super().exception()
            traceback = super().traceback()
            if exception is None:
                traceback = self.report.traceback
            return traceback

    class TaskExecutor:
        def __init__(self, *, throw=False):
            self.throw = throw

        def submit(self, request):
            report = None
            e = None
            tb = None
            try:
                report = request.task(*request.args, **request.kwargs) # Task returns Report to its contained Request
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if self.throw:
                    raise exc_value.with_traceback(exc_traceback)
                tb_lines = traceback.format_tb(exc_traceback)
                tb = '\n'.join(tb_lines)
                logger.error(f"\n{tb}{exc_type}: {exc_value}")
                e = exc_value
            future = Logging.TaskReportFuture(report, exception=e, traceback=tb)
            return future

        def restart(self):
            pass

    class Response(Response):
        def __init__(self,
                     *,
                     pool,
                     request,
                     future,
                     start_time,
                     done_callback=None,
                     task_key=None,
                     task_id=None,
                     task_logspace=None,
                     task_logname=None):
            self.pool = pool
            self.request = request
            self.future = future
            self.start_time = start_time

            self.args_responses = None
            self.kwargs_responses = None

            # HACK
            # RENAME: promise --> response
            self.future.promise = self

            self.done_time = None
            self.done_callbacks = []
            self._future_result = None
            self._result = None
            self._done = False
            future.add_done_callback(self._done_callback)
            if done_callback is not None:
                self.done_callbacks.append(done_callback)
            if self.future.done():
                done_callback(self)
            self.key = task_key
            self.id = task_id
            self.logspace = task_logspace
            self.logname = task_logname


        def __str__(self):
            tag = tag.Tagger().str_ctor(self.__class__,
                                           pool=self.pool,
                                           request=self.request,
                                           future=self.future)
            return tag

        def __repr__(self):
            return str(self)

        @property
        def task(self):
            return self.request.func

        def exception(self):
            exception = self.future.exception()
            return exception

        def traceback(self):
            traceback = self.future.traceback()
            return traceback

        def result(self):
            if self._result is None:
                self._result = self.future.result()
            return self._result

        @property
        def done(self):
            if hasattr(self.future, 'done'):
                return self.future.done()
            return self._done

        @property
        def running(self):
            if hasattr(self.future, 'running'):
                return self.future.running()
            return not self.failed and not self.done

        def report(self):
            # NB: Task.__call__() returns Report from the inner Request's evaluation Result.
            report = self.future.report
            return report

        def logfile(self):
            try:
                _path = self.logpath
                _f = self.request.logspace.filesystem.open(_path, 'r')
            except:
                return None
            return _f

        def log(self, size=None):
            logfile = self.logfile()
            if logfile is None:
                r = ''
            else:
                if size is None:
                    r = logfile.read()
                else:
                    r = logfile.read(size)
            return r

        @staticmethod
        def _done_callback(future):
            promise = future.promise
            done_time = datetime.datetime.now()
            promise._done = True
            promise.done_time = done_time
            for c in promise.done_callbacks:
                c(future.promise)

    class Request(Request):
        def __init__(self, pool, request):
            self.pool = pool
            self.request = request
            self.task = pool.Task(pool, request)
            super().__init__(self.task, *request.args, **request.kwargs)

        def __tag__(self):
            _ = tag.tag(self.request)
            return _

        def __repr__(self):
            _ = tag.Tagger().repr_ctor(self.__class__, self.pool, self.request)
            return _

        def __str__(self):
            _request_str = str(self.request)
            pool_str = str(self.pool)
            _ = f"{_request_str}.apply({pool_str})"
            return _

        def with_lifecycle_callback(self, lifecycle_callback):
            self.request = self.request.with_lifecycle_callback(lifecycle_callback)
            _ = super().with_lifecycle_callback(lifecycle_callback)
            return _

        def with_throw(self, throw=True):
            self.request = self.request.with_throw(throw)
            _ = super().with_throw(throw)
            return _

        def rebind(self, *args, **kwargs):
            _request = self.request.rebind(*args, **kwargs)
            request = self.__class__(self.pool, _request)
            return request

        def evaluate(self, **task_trace):
            r = self.pool.evaluate(self, **task_trace)
            return r

        @property
        def id(self):
            return self.task.id

        @property        
        def key(self):
            return self.task.key

        @property
        def logpath(self):
            return self.task.logpath

    def __init__(self,
                 name='Logging',
                 *,
                 dataspace,
                 priority=0,
                 return_none=False,
                 validate_tasks=False,
                 throw=True,
                 log_to_file=True,
                 log_level='INFO',
                 log_prefix=None,
                 log_format="%(asctime)s:%(levelname)s:%(funcName)s:%(message)s",
                 redirect_stdout=False):
        state = name,\
                dataspace,\
                priority,\
                return_none,\
                validate_tasks, \
                throw,\
                log_to_file,\
                log_level,\
                log_prefix,\
                log_format,\
                redirect_stdout
        Logging.__setstate__(self, state)

    def __getstate__(self):
        state = self.name,\
                    self.dataspace,\
                    self.priority,\
                    self.return_none,\
                    self.validate_tasks, \
                    self.throw, \
                    self.log_to_file,\
                    self.log_level,\
                    self.log_prefix,\
                    self.log_format,\
                    self.redirect_stdout
        return state

    def __setstate__(self, state):
        self.name, \
        self.dataspace,\
        self.priority,\
        self.return_none,\
        self.validate_tasks, \
        self.throw, \
        self.log_to_file,\
        self.log_level,\
        self.log_prefix,\
        self.log_format,\
        self.redirect_stdout = state

        self.anchorchain = 'datablocks', 'eval', 'pool', 'Logging'
        if self.name:
            self.anchorchain = self.anchorchain + (self.name,)
        self.anchorspace = self.dataspace.subspace(*self.anchorchain).ensure()

        self._executor = None
        surname = self.__class__.__name__.lower()
        self.logpath = surname if self.name is None else os.path.join(surname, self.name)

        self._ids = None

    @property
    def ids(self):
        from ..config import CONFIG
        from ..db import Ids
        if self._ids is None:
            self._ids = Ids(self.anchorspace,
              user=CONFIG.USER,
              postgres=CONFIG.POSTGRES,
              postgres_schemaname="request",
              postgres_tablename="ids")
        return self._ids

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.name == other.name and \
            self.dataspace == other.dataspace and \
            self.priority == other.priority and \
            self.return_none == other.return_none and \
            self.validate_tasks == other.validate_tasks and \
            self.throw == other.throw and\
            self.log_level == other.log_level and\
            self.log_prefix == other.log_prefix and \
            self.log_format == other.log_format and\
            self.log_to_file == other.log_to_file and \
            self.redirect_stdout == other.redirect_stdout

    def _tag_(self):
        tag = f"{tag.Tagger().ctor_name(self.__class__)}({self.name}, " +\
                                                            f"dataspace={self.dataspace})"
        return tag

    def __str__(self):
        s = f"{tag.Tagger().ctor_name(self.__class__)}({self.name+', ' if self.name else ''}" + \
              f"dataspace={self.dataspace})"
        return s

    def __repr__(self):
        repr = f"{tag.Tagger().ctor_name(self.__class__)}({self.name}, " + \
               f"dataspace={self.dataspace}, " + \
               f"priority={self.priority}, " + \
               f"return_none={self.return_none}, " + \
               f"validate_tasks={self.validate_tasks}, " + \
               f"throw={self.throw}, " + \
               f"log_to_file={self.log_to_file}, " + \
               f"log_level={self.log_level}, "+ \
               f"log_prefix={self.log_prefix}, "+ \
               f"log_format={self.log_format}, "+ \
               f"redirect_stdout={self.redirect_stdout})"
        return repr

    def restart(self):
        self.executor.restart()

    def apply(self, request):
        return self.Request(self, request)

    def task_key(self, request):
        return repr(request)

    def task_id(self, key, *, version=VERSION, unique_hash=True):
        if self.validate_tasks:
            key_ = self.ids.sanitize_value(key, quote=False)
            id = self.ids.get_id(key_, version, unique_hash=unique_hash)  # TODO: do we need to have a unique hash?
        else:
            namespace_bytes = key.encode()
            """
            #namespace_uuid = uuid.UUID(bytes=namespace_bytes[:16]).int
            namespace_uuid = uuid.UUID(bytes=namespace_bytes)
            _uuid = uuid.uuid5(namespace_uuid, key).int
            id = _uuid + version
            """
            hashstr = hashlib.sha1(namespace_bytes).hexdigest()
            maxint64 = int(2**63)
            id = int(hashstr, 16)%maxint64
        return id

    @staticmethod
    def timeus():
        now = datetime.datetime.now()
        td = pd.Timestamp(now) - pd.Timestamp(0)
        ts = int(td.total_seconds())
        us = td.microseconds
        tus = int(ts * 1e+6 + us)
        return tus

    # TODO: deprecate?
    def tag(self, key, *, version=None, unique_hash=False):
        if version is None:
            tus = self.timeus()
            version = tus
        tag = self.id(key, version=version, unique_hash=unique_hash)
        return tag

    def validate_task(self, id, key, request_tag, request_repr):
        if not self.validate_tasks:
            return
        from ..config import CONFIG
        from ..db import Ids
        # REMOVE
        # TODO: use self.ids?
        ids = Ids(self.anchorspace,
                   user=CONFIG.USER,
                   postgres=CONFIG.POSTGRES,
                   postgres_schemaname="request",
                   postgres_tablename="ids")
        dataspace_ = ids.sanitize_value(str(self.anchorspace), quote=False)
        key_ = ids.sanitize_value(key, quote=False)
        _dataspace, _key, _version = ids.lookup_id(id)
        if _dataspace != dataspace_:
            raise ValueError(
                f"Task id {id} pool dataspace mismatch in task for request {request_repr}\n\ttag: {request_tag}:\n\tkey: {key}\n\texpected:  {dataspace_}\n\tlooked up: {_dataspace} in db {db}")
        if _key != key_:
            raise ValueError(
                f"Task id {id} normalized key mismatch in task for request {request_repr}\n\ttag {request_tag}:\n\texpected: {key_}\n\tlooked up:{_key} in db {self.db}")
        logging.info(
            f"Validated task id {id} from db {self.db} for request {request_repr}\n\ttag {request_tag}\n\tdataspace: {_dataspace}\n\tkey: {_key}\n\tversion: {_version}")

    # TODO: move to ctor or REMOVE: use ids instead?
    @property
    def db(self):
        from ..config import CONFIG
        return CONFIG.POSTGRES['db']

    @property
    def executor(self):
        if self._executor is None:
            self._executor = Logging.TaskExecutor(throw=self.throw)
        return self._executor

    def evaluate(self, request, **task_trace):
        # FIX: task_trace must match those imposed by task (key, id, logspace, logname)
        assert isinstance(request, self.__class__.Request)
        task = request.task.clone()
        """
        if ('task_key' in task_trace and task_trace['task_key'] != task.key) or \
           ('task_id' in task_trace and task_trace['task_id'] != task.id) or \
           ('task_logspace' in task_trace and task_trace['task_logspace'] != task.logspace) or\
           ('task_logname' in task_trace and task_trace['task_logname'] != task.logname)
            logging.warning(f"Ignoring mismatched task_trace settings: {task_trace}")
        """

        _task_trace = dict(task_key=task.key, task_id=task.id, task_logspace=task.logspace, task_logname=task.logname)
        if task_trace != _task_trace:
            logging.debug(f"Ignoring mismatch: task_trace <-- {task_trace} !!!!!!!!!!========= {_task_trace} --> _task_trace")

        delayed = self._delay_request(request.with_throw(self.throw))
        future = self._submit_delayed(delayed)
        start_time = datetime.datetime.now()
        logger.debug(f"Submitted delayed request based on task "
                     f"with id {task.id} "
                     f"to evaluate request {request} at {start_time}"
                     f" with prority {self.priority}"
                     f" logging to {task.logpath}")
        response = self.Response(pool=self,
                                 request=request,
                                 future=future,
                                 start_time=start_time,
                                 done_callback=self._execution_done_callback,
                                 **_task_trace)
        return response

    def compute(self, request, **task_trace):
        promise = self.evaluate(request, **task_trace)
        result = promise.result()
        return result

    def as_completed(self, promises):
        for p in promises:
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
        '''
            logger.debug(f"Delaying arg[{index}] for  request {request}\n\t"
                         f"arg[{index}]: {arg}")
            '''
        delayed = self._delay_request_argument(request, arg)
        return delayed

    def _delay_request_kwarg(self, request, kwarg, key):
        '''
            logger.debug(f"Delaying kwarg[{key}] for  request {request}\n\t"
                         f"kwarg[{key}]: {kwarg}")
             '''
        delayed = self._delay_request_argument(request, kwarg)
        return delayed

    def _delay_request_argument(self, request, arg):
        result = self._delay_request(arg) if isinstance(arg, self.__class__.Request) else arg
        return result

    @staticmethod
    def _execution_done_callback(promise):
        pass

"""
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
                 priority=0,
                 return_none=False,
                 validate_tasks=False,
                 throw=False,
                 log_to_file=True,
                 log_level='INFO',
                 log_prefix=None,
                 log_format="%(asctime)s:%(levelname)s:%(funcName)s:%(message)s",
                 redirect_stdout=False):
        _kwargs = dict(dataspace=dataspace,
                       priority=priority,
                       return_none=return_none,
                       validate_tasks=validate_tasks,
                       throw=throw,
                       log_to_file=log_to_file,
                       log_level=log_level,
                       log_prefix=log_prefix,
                       log_format=log_format,
                       redirect_stdout=redirect_stdout)
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
        arequest = self.Request(self, request, dask_key)
        return arequest

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

    def as_completed(self, promises):
        futures = (p.future for p in promises)
        for f in dd.distributed.as_completed(futures):
            yield f.promise

    def _submit_delayed(self, delayed):
        future = self.client.compute(delayed)
        return future

    def _delay_request(self, request):
        task = request.task
        _args, _kwargs = self._delay_request_args_kwargs(request)
        delayed = dask.delayed(task)(*_args,
                                             dask_key_name=request.dask_key,
                                              **_kwargs)
        return delayed

    def _delay_request_argument(self, request, arg):
        delayed = self._delay_request(arg) if isinstance(arg, Dask.Request) else arg
        return delayed
"""

class Ray(Logging):
    class Task(Logging.Task):
        pass

    class Request(Logging.Request):
        pass

    class Response(Logging.Response):
        pass

    def __init__(self,
                 name='Ray',
                 *,
                 dataspace,
                 ray_kwargs=None,
                 ray_working_dir_config={},
                 validate_tasks=False,
                 throw=False,
                 log_to_file=True,
                 log_level='INFO',
                 log_prefix=None,
                 log_format="%(asctime)s:%(levelname)s:%(funcName)s:%(message)s",
                 redirect_stdout=False,
                ):

        _kwargs = dict(dataspace=dataspace,
                       return_none=False,
                       priority=0,
                       validate_tasks=validate_tasks,
                       throw=throw,
                       log_to_file=log_to_file,
                       log_level=log_level,
                       log_prefix=log_prefix,
                       log_format=log_format,
                       redirect_stdout=redirect_stdout)
        super().__init__(name, **_kwargs)
        self.ray_kwargs = ray_kwargs
        self._ray_client = None
        self.ray_working_dir_config = ray_working_dir_config

    def __repr__(self):
        repr = f"{tag.Tagger.ctor_name(self.__class__)}({self.name}, " + \
               f"dataspace={self.dataspace}, " + \
               f"ray_kwargs={self.ray_kwargs}, " + \
               f"ray_working_dir_config={self.ray_working_dir_config}, " + \
               f"validate_tasks={self.validate_tasks}, " + \
               f"throw={self.throw}, " + \
               f"log_to_file={self.log_to_file}, " + \
               f"log_level={self.log_level}, "+ \
               f"log_prefix={self.log_prefix}, "+ \
               f"log_format={self.log_format}, "+ \
               f"redirect_stdout={self.redirect_stdout})"
        return repr

    def __delete__(self):
        if self._ray_client is not None:
            self._ray_client.disconnect()
        self._ray_client = None

    def __getstate__(self):
        state = super().__getstate__(), \
                self.ray_kwargs, \
                self.ray_working_dir_config
        return state

    def __setstate__(self, state):
        _state, \
        self.ray_kwargs, \
        self.ray_working_dir_config = state
        super().__setstate__(_state)
        self._ray_client = None

    def apply(self, request):
        arequest = self.Request(self, request)
        return arequest

    def repr(self, request):
        pool_key = tag.Tagger(tag_defaults=False) \
            .repr_ctor(self.__class__,
                       self.name,
                       dataspace=self.dataspace,
                       ray_kwargs=self.ray_kwargs,
                       ray_working_dir_config=self.ray_working_dir_config)
        key = f"{pool_key}[[{request.tag}]]"
        return key

    @property
    def client(self):
        from ..config import CONFIG
        from .raybuilder import get_ray_client
        if self._ray_client is None:
            namespace = f"{CONFIG.USER}.{self.name}"
            self._ray_client = get_ray_client(namespace=namespace,
                                          ray_kwargs=self.ray_kwargs,
                                          ray_working_dir_config=self.ray_working_dir_config)
        return self._ray_client

    @property
    def executor(self):
        return self._ray_client

    def restart(self):
        self.__delete__()
        _ = self.client

    def as_completed(self, promises):
        futures = (p.future for p in promises)
        for f in concurrent.futures.as_completed(futures):
            yield f.promise

    def evaluate(self, request, **task_trace):
        assert isinstance(request, self.__class__.Request)
        task = request.task.clone()
        client = self.client
        if client is None:
            future = ray.remote(Ray._delayed) \
                .remote(task, *request.args, **request.kwargs) \
                .future()
        else:
            with self.client:
                future = ray.remote(Ray._delayed)\
                    .remote(task, *request.args, **request.kwargs)\
                    .future()
        start_time = datetime.datetime.now()
        logpath = task.logpath
        logger.info(f"Submitted task "
                     f"with id {task.id} "
                     f"from {self.db} "
                     f"to evaluate request {request} at {start_time}"
                     f" with prority {self.priority}"
                     f" logging to {logpath}")
        response = self.Response(pool=self,
                                 request=request,
                                 future=future,
                                 start_time=start_time,
                                 done_callback=self._execution_done_callback,
                                 **task_trace)
        return response

    @staticmethod
    def _delayed(task, *args, **kwargs):
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
                 priority=0,
                 return_none=False,
                 validate_tasks=False,
                 log_to_file=True,
                 log_level='INFO',
                 log_prefix=None,
                 log_format="%(asctime)s:%(levelname)s:%(funcName)s:%(message)s",
                 redirect_stdout=False):
        _kwargs = dict(dataspace=dataspace,
                         priority=priority,
                         return_none=return_none,
                         validate_tasks=validate_tasks,
                         log_to_file=log_to_file,
                         log_level=log_level,
                         log_prefix=log_prefix,
                         log_format=log_format,
                         redirect_stdout=redirect_stdout)
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

    def as_completed(self, promises):
        futures = (p.future for p in promises)
        for f in concurrent.futures.as_completed(futures):
            yield f.promise


class HTTP(Logging):
    class Future(Logging.TaskReportFuture):
        # HTTP inherits _delay_request() from Logging, whose 'delayed' is a Task  returning Report
        # so we need this Future to be able to unpack it.
        def __init__(self, result, *, exception=None, traceback=None):
            super().__init__(result, exception=exception, traceback=traceback)

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
                logger.debug(f"Added authorization to headers {headers} for pool url {self.pool.url}")
            return headers

        def submit(self, request):
            result = None
            exception = None
            traceback = None
            request_pickle = pickle.dumps(request)
            if self.pool.url is not None:
                headers = self.add_auth_header(self.pool.auth, {})
                http_response = requests.post(self.pool.url+'/eval', headers=headers, data=request_pickle)
                logger.debug(f"Response status_code: {http_response.status_code}")
                response_pickle = http_response.content
            else:
                response_pickle = serve._eval(request_pickle, throw=self.pool.throw)
            response = pickle.loads(response_pickle)
            if 'result' in response:
                # Assume that result is a Report, just like in evaluating a Task
                result = pickle.loads(response['result'])
                # HACK: tag is broken
                #logger.debug(f"Response result: {result}")
            if 'exception' in response:
                exception = pickle.loads(response['exception'])
                logger.debug(f"Response exception: {exception}")
            if 'traceback' in response:
                traceback = pickle.loads(response['traceback'])
                logger.debug(f"Response traceback: {traceback}")
            future = HTTP.Future(result, exception=exception, traceback=traceback)
            return future

    def __init__(self,
                 name='http',
                 *,
                 dataspace,
                 url=None,
                 auth=None,
                 priority=0,
                 return_none=False,
                 validate_tasks=False,
                 throw=False,
                 log_to_file=True,
                 log_level='INFO',
                 log_prefix=None,
                 log_format="%(asctime)s:%(levelname)s:%(funcName)s:%(message)s",
                 redirect_stdout=False):
        _state = name, \
                dataspace, \
                priority, \
                return_none, \
                validate_tasks, \
                throw, \
                log_to_file, \
                log_level, \
                log_prefix, \
                log_format, \
                redirect_stdout
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
        repr = f"{tag.Tagger.ctor_name(self.__class__)}({self.name}, " + \
               f"dataspace = {self.dataspace}, " + \
               f"url = {self.url}, " + \
               f"auth = {self.auth}, " + \
               f"priority = {self.priority}, " + \
               f"return_none = {self.return_none}, " + \
               f"validate_tasks = {self.validate_tasks}, " + \
               f"throw = {self.throw}, " + \
               f"log_to_file = {self.log_to_file}, " + \
               f"log_level={self.log_level}, " + \
               f"log_prefix={self.log_prefix}, " + \
               f"log_format={self.log_format}, " + \
               f"redirect_stdout={self.redirect_stdout})"
        return repr

    def repr(self, request):
        pool_key = tag.Tagger(tag_defaults=False) \
            .repr_ctor(self.__class__, self.name, dataspace=self.dataspace, url=self.url)
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


DATABLOCKS_LOGGING_POOL = Logging(dataspace=DATABLOCKS_DATALAKE)


def print_all_promises(tasks):
    import time
    print(len(tasks))
    for i, t in enumerate(tasks):
        print(f"{i}: {t.tag}: {t}: start_time: {t.start_time}")
        time.sleep(0.03)


def done_running_failed_pending_promises(tasks, include_failed=True, *, delay_secs=0.0, details=False):
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


def ipromise_results(itasks):
    print(f"{len(itasks)}")
    for i, t in itasks.items():
        run_time = t.done_time - t.start_time if t.done_time is not None else 0
        tag = repr(t.request)
        print(f"{i}: {tag}: {t.result()}: run_time: {run_time}")


def ipromise_logs(itasks):
    import time
    print(f"{len(itasks)}")
    for i, t in itasks.items():
        print(f"{i}: {repr(t.request)}: {t}")
        print(t.log(position=0, size=10000000))
        print("****************")
        time.sleep(0.1)


