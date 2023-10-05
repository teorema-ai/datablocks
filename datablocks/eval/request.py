import collections
import copy
import datetime
import enum
import importlib
import inspect
import logging
import pdb
import time

from .. import utils
from .. import signature
from ..utils import REPLACE, REMOVE, DEPRECATED, ALIAS, truncate_str

# eval support
_eval = __builtins__['eval']
import datablocks.dataspace

logger = logging.getLogger(__name__)


class Future:
    def __init__(self, func, *args_responses, **kwargs_responses):
        self.func = func
        self.args_responses = args_responses
        self.kwargs_responses = kwargs_responses
        self._running = True
        self._done = False
        self._exception = None
        self._traceback = None
        self._result = None
        self._done_callback = None

    def __str__(self):
        return signature.Tagger().str_ctor(self.__class__, self.func, *self.args_responses, **self.kwargs_responses)
    
    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__, self.func, *self.args_responses, **self.kwargs_responses)

    def result(self):
        if not self._done:
            try:
                args = [self._arg_result(arg_response) for arg_response in self.args_responses]
                kwargs = {key: self._arg_result(kwarg_response) for key, kwarg_response in self.kwargs_responses.items()}
                #DEBUG
                #pdb.set_trace()
                self._result = self.func(*args, **kwargs)
            except KeyboardInterrupt as k:
                raise(k)
            except Exception as e:
                _, exc_value, exc_traceback = utils.exc_info()
                self._exception = exc_value
                self._traceback = exc_traceback
            finally:
                self._done = True
                self._running = False
                if self._done_callback is not None:
                    self._done_callback(self)
        if self._exception:
            raise self._exception.with_traceback(self._traceback)
        return self._result

    def done(self):
        return self._done

    def running(self):
        return self._running

    def exception(self):
        return self._exception

    def traceback(self):
        return self._traceback

    def add_done_callback(self, callback):
        self._done_callback = callback
        if self.done():
            self._done_callback(self)

    @staticmethod
    def _arg_result(arg):
        if not isinstance(arg, Report) and not isinstance(arg, Response):
            result = arg
        else:
            exception = arg.exception()
            if exception is not None:
                raise exception
            result = arg.result()
        return result

class Task:
    class Lifecycle(enum.IntEnum):
        ERROR = -1
        BEGIN = 0
        END = 1

    def __init__(self, func):
        self.func = func
        self.key = None
        self.id = None
        self.logspace = None
        self.logpath = None
        self.logname = None

    def __call__(self, *args, **kwargs):
        _ = self.func(*args, **kwargs)
        return _

    def __repr__(self):
        repr = signature.Tagger().repr_ctor(self.__class__, self.func)
        return repr

    def __str__(self):
        str = signature.Tagger().str_ctor(self.__class__, self.func)
        return str
    

# TODO: split into RPCClass (holding ctor + ctor_args, ctor_kwargs)
# TODO: and RPCMethod holding an instance of RPCClass and _method_, _prototype_
class RPC:
    _method_ = None
    _prototype_ = None
    """
       * This is a callable that will instantiate an object from {ctor_url} (fully-qualified ctor name) and call its {method}.
       * Before the call is made the {method}'s signature is verified by comparing to {self.signature} and
         {self.__defaults__} and {self.__kwdefaults__} are used to compute the {tag}.
         - self.signature, self.__defaults__ and self.__kwdefaults__ can be overriden in a subclass
         - or are obtained from {self.prototype}
         - which is equal to the first non-None of
            x {self._prototype_}
            x {self.{self._method_}}
        * Observe that {self._method_} and {self._prototype_} can be set with
          - .with_method(method)
          - .with_prototype(prototype)
    """
    def __init__(self, ctor_fqn, *, args=None, kwargs=None, _tag_=None, _str_=None):
        self.ctor_url = ctor_fqn
        self.ctor_args = args or []
        self.ctor_kwargs = kwargs or {}
        self._check_signature_ = False
        self._tag = _tag_
        self._str = _str_
        self._instance = None
        self._func = None

    @property
    def __defaults__(self):
        defaults = self.prototype.__defaults__
        return defaults

    @property
    def __kwdefaults__(self):
        kwdefaults = self.prototype.__kwdefaults__
        return kwdefaults

    @property
    def signature(self):
        signature = inspect.signature(self.prototype)
        return signature

    @property
    def prototype(self):
        if self._prototype_ is not None:
            prototype = self._prototype_
        else:
            prototype = getattr(self, self.method)
        return prototype

    @property
    def method(self):
       return self._method_

    def with_method(self, method):
        self._method_ = method
        return self

    def with_prototype(self, prototype):
        self._prototype_ = prototype
        return self

    def with_signature_checking(self, check=True):
        self._check_signature_ = check
        return self

    def __tag__(self):
        if self._tag is None:
            _tag = repr(self)
            if hasattr(self, '_method_'):
                self._tag = f"{_tag}.with_method({repr(self._method_)})"
            else:
                self._tag = _tag
        return self._tag

    def __str__(self):
        if self._str is not None:
            _str = self._str
        else:
            tagger = signature.Tagger(tag_defaults=False)
            ctor_args_strs = [tagger.str_object(arg) for arg in self.ctor_args]
            ctor_kwargs_strs = {key: tagger.str_object(arg) for key, arg in self.ctor_kwargs.items()}
            _str = tagger.str_ctor(RPC,
                                   self.ctor_url,
                                   args=ctor_args_strs,
                                   kwargs=ctor_kwargs_strs)
        if hasattr(self, '_method_'):
            str = f"{_str}.with_method({repr(self._method_)})"
        else:
            str = _str
        return str

    def __repr__(self):
        _repr = signature.Tagger().repr_ctor(RPC,
                                         self.ctor_url,
                                         args=self.ctor_args,
                                         kwargs=self.ctor_kwargs)
        if hasattr(self, '_method_'):
            repr_ = f"{_repr}.with_method({repr(self._method_)})"
        else:
            repr_ = _repr
        return repr_

    def __call__(self, *args, **kwargs):
        r = self.func(*args, **kwargs)
        return r

    @property
    def ctor(self):
        urlparts = self.ctor_url.split('.')
        modname = '.'.join(urlparts[:-1])
        ctorname = urlparts[-1]
        mod = importlib.import_module(modname)
        ctor = getattr(mod, ctorname)
        return ctor

    @property
    def func(self):
        if self._func is None:
            logger.debug(f"Instantiating {self.ctor}")
            logger.debug(f"Ctor signature: {inspect.signature(self.ctor)}")
            logger.debug(f"Ctor module: {inspect.getmodule(self.ctor)}")
            logger.debug(f"method: {self._method_}")
            self._instance = self.ctor(*self.ctor_args, **self.ctor_kwargs)
            self._func = getattr(self._instance, self._method_)
        if self._check_signature_:
            assert inspect.signature(self._func) == self.signature
        return self._func

    def __getstate__(self):
        return self.ctor_url, self.ctor_args, self.ctor_kwargs, self._method_, self._prototype_, self._check_signature_, self._str, self._tag

    def __setstate__(self, state):
        self.ctor_url, self.ctor_args, self.ctor_kwargs, self._method_, self._prototype_, self._check_signature_, self._str, self._tag = state
        self._tag, self._func = None, None


class URL_RPC(RPC):
    @property
    def func(self):
        return self.ctor


class Request:
    def __init__(self, func, *args, **kwargs):
        #DEBUG
        #pdb.set_trace()
        if isinstance(func, Task): # implement rebind without changing task to preserve key, id, logname, etc.
            self.task = func
        else:
            if isinstance(func, str):
                _func = URL_RPC(func)
            else:
                _func = func
            if not callable(_func):
                raise ValueError(f"Request func {_func} of non-callable type {type(_func)}")
            self.task = Task(_func)
        self.args, self.kwargs = args, kwargs
        self.settings = dict(
                             throw=False, 
                             lifecycle_callback=None,
                             )
    def __ne__(self, other):
        _ = (self.__class__ != other.__class__) or \
            (signature.tag(self) != signature.tag(other))
        return _

    @ALIAS
    def set(self, **settings):
        _ = self.with_settings(**settings)
        return _
    
    def get(self, key):
        _ = self.settings[key]
        return _

    def has(self, key):
        _ = key in self.settings
        return _
    
    # Use .set()
    @DEPRECATED
    def with_settings(self, **settings):
        request = copy.deepcopy(self)
        request.settings.update(**settings)
        return request

    # TODO: unify callbacks (summary, lifecycle) inside `settings`
    # Use .set()
    def with_summary(self, summary):
        self.set(summary=summary)
        return self

    def with_lifecycle_callback(self, lifecycle_callback):
        """
            lifecycle_callback: (Task.Lifecycle, request, Option[response] -> None)
        """
        request = self.set(lifecycle_callback=lifecycle_callback)
        return request

    def with_throw(self, throw=True):
        request = self.set(throw=throw)
        return request

    @ALIAS
    def with_evaluate_raises_exceptions(self, raises=True):
        request = self.with_throw(raises)
        return request

    @property
    def evaluate_raises_exceptions(self):
        return self.settings['throw']
    
    @property
    def lifecycle_callback(self):
        return self.settings['lifecycle_callback']

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.task == other.task and \
               self.args == other.args and self.kwargs == other.kwargs

    def redefine(self, func, *args, **kwargs):
        request = self.__class__(func, *args, **kwargs)\
                    .with_settings(**self.settings)
        return request

    def rebind(self, *args, **kwargs):
        # rebind should preserve task
        request = self.__class__(self.task, *args, **kwargs)\
                    .with_settings(**self.settings)
        return request

    def __str__(self):
        str = signature.Tagger().str_func(self.task.func, *self.args, **self.kwargs)
        return str

    def __repr__(self):
        repr = signature.Tagger().repr_func(self.task.func, *self.args, **self.kwargs)
        return repr

    def __tag__(self):
        _ = signature.Tagger().tag_func(self.task.func, *self.args, **self.kwargs)
        return _

    def iargs_kargs_kwargs(self):
        if hasattr(self.task, '__iargs_kargs_kwargs__'):
            return self.task.__iargs_kargs_kwargs__
        iargs, kargs, kwargs = \
          signature.func_iargs_kargs_kwargs(self.task.func,
                                        False,
                                        True,
                                        *self.args,
                                        **self.kwargs)
        return iargs, kargs, kwargs

    def evaluate_args_kwargs(self, args, kwargs):
        _args = [self._evaluate_arg(a, i) for i, a in enumerate(args)]
        _kwargs = {k: self._evaluate_kwarg(a, k) for k, a in kwargs.items()}
        return _args, _kwargs

    def _evaluate_arg(self, arg, index):
        '''
            logger.debug(f"Computing args[{index}] for request tagged\n\t{self.tag}\n"
                        f"\t\targs[{index}] {arg}")
                    '''
        r = self._evaluate_argument(arg)
        '''logger.debug(f"\n\t\tDone with args[{index}]")'''
        return r

    def _evaluate_kwarg(self, kwarg, key):
        '''
            logger.debug(f"Computing kwargs[{key}] for request tagged\n\t{self.tag}\n"
                        f"\t\tkwargs[{key}]  {kwarg}")
                    '''
        r = self._evaluate_argument(kwarg)
        '''logger.debug(f"\n\t\tDone with kwargs[{key}]")'''
        return r

    # TODO: eliminate 'report' option?  It doesn't seem to be use5
    def _evaluate_argument(self, arg):
        response = \
                    arg.evaluate() if isinstance(arg, BLOCK.Request) else \
                    arg.evaluate() if isinstance(arg, Request) else \
                    arg
        return response

    def close(self):
        c = Closure(self)
        return c

    def apply(self, functor, *args, **kwargs):
        if functor is None:
            return self
        request = self
        a = functor.apply(request, *args, **kwargs)
        return a

    def through(self, functor, *args, **kwargs):
        if functor is None:
            return self
        if hasattr(functor, 'through'):
            return functor.through(self)
        _args = [a.through(functor, *args, **kwargs)
                 if isinstance(a, Request) else a
                 for a in self.args]
        _kwargs = {k: a.through(functor, *args, **kwargs)
        if isinstance(a, Request) else a
                   for k, a in self.kwargs.items()}
        request = self.rebind(*_args, **_kwargs)
        t = functor.apply(request, *args, **kwargs)
        return t

    def compute(self):
        response = self.evaluate()
        result = response.result()
        return result

    def evaluate(self):
        args_responses, kwargs_responses = self.evaluate_args_kwargs(self.args, self.kwargs)
        future = Future(self.task, *args_responses, **kwargs_responses)
        response = Response(request=self, future=future)
        future.promise = response
        if self.lifecycle_callback is not None:
            self.lifecycle_callback(Task.Lifecycle.BEGIN, self, response)
        return response


class Proxy(Request):
    def __init__(self, request):
        self.request = request

    def __ne__(self, other):
        return self.request.__ne__(other)
    
    def set(self, **settings):
        raise ValueError(f"Cannot 'set' on a Proxy request")

    def get(self, key):
        return self.request.get(key)
    
    def __eq__(self, other):
        return self.request.__eq__(other)

    def redefine(self, func, *args, **kwargs):
        raise ValueError("Cannot redefine a Proxy request")

    def rebind(self, *args, **kwargs):
        raise ValueError("Cannot rebind a Proxy request")
    
    def __str__(self):
        return self.request.__str__()
    
    def __repr__(self):
        return self.request.__repr__()

    def __tag__(self):
        return self.request.__tag__()
    
    def apply(self, functor, *args, **kwargs):
        request = self.request.apply(functor, *args, **kwargs)
        wrapper = self.__class__(request)
        return wrapper

    def through(self, functor, *args, **kwargs):
        request = self.request.through(functor, *args, **kwargs)
        wrapper = self.__class__(request)
        return wrapper

    def evaluate(self):
        _ = self.request.evaluate()
        return _


# TODO: make an extension of dict for easy serdes, including RPC
class Report:
    class STATUS(enum.IntEnum):
        UNKNOWN = 0
        RUNNING = 1
        SUCCEEDED = 2
        FAILED = 3
        PENDING = 4

        def complete(self):
            return (self == Report.STATUS.SUCCEEDED or self == Report.STATUS.FAILED)
        
        def incomplete(self):
            return (self == Report.STATUS.PENDING or self == Report.STATUS.RUNNING)
        
        def unknown(self):
            return (self == Report.STATUS.UNKNOWN)

    def __init__(self, response):
        self.request = response.request
        self.start_time = response.start_time
        self.status = Report.STATUS.UNKNOWN
        # TODO: --> result(), exception() for conformity with Response
        self.result = None
        self.exception = None
        self.traceback = None
        if response.running:
            self.status = Report.STATUS.RUNNING
        else:
            if response.done:
                if response.failed:
                    self.status = Report.STATUS.FAILED
                    try:
                        self.exception = response.exception()
                        self.traceback = response.traceback()
                    except:
                        pass
                else:
                    try:
                        self.result = response.result()
                        self.status = Report.STATUS.SUCCEEDED
                    except:
                        pass
            else:
                self.status = Report.STATUS.PENDING
        self.id = response.id
        self.logpath = response.logpath
        self.logspace = response.logspace
        self.logspace_url = self.logspace.url if self.logspace is not None else None
        if hasattr(response, 'done_time'):
            self.done_time = response.done_time
        else:
            self.done_time = None
        self.args_reports = [self._arg_report(r) for r in response.args_responses] if response.args_responses is not None else []
        self.kwargs_reports = {k: self._arg_report(v) for k, v in response.kwargs_responses.items()} if response.kwargs_responses is not None else {}
        self.settings = dict(result_use_summary=False)

    def set(self, **settings):
        self.settings.update(**settings)
        return self

    def summary(self):
        tagger = signature.Tagger()
        report = self
        args_summaries = [report._arg_summary(r) for r in report.args_reports]
        kwargs_summaries = {k: report._arg_summary(v) for k, v in report.kwargs_reports.items()}
        summary = dict(id=f"id:{report.id}",
                       completed=(report.status in [Report.STATUS.SUCCEEDED, Report.STATUS.FAILED]),
                       success=(report.status in [Report.STATUS.SUCCEEDED]),
                       status=str(report.status),
                       exception=str(report.exception),
                       traceback=utils.exc_traceback_string(report.traceback),
                       start_time=str(report.start_time),
                       done_time=str(report.done_time),
                       logpath=report.logpath,
                       logspace_url=report.logspace_url,
                       logspace=str(report.logspace),
                       args_summaries=args_summaries,
                       kwargs_summaries=kwargs_summaries,
                       #request=tagger.str_object(report.request),
                       request=tagger.repr_object(report.request),
                       )
        total_logs = 0
        total_logs += sum([s['logs_total'] for s in args_summaries if 'logs_total' in s])
        total_logs += sum([s['logs_total'] for s in kwargs_summaries.values() if 'logs_total' in s])
        valid_logs = 0
        valid_logs += sum([s['logs_valid'] for s in args_summaries if 'logs_valid' in s])
        valid_logs += sum([s['logs_valid'] for s in kwargs_summaries.values() if 'logs_valid' in s])
        
        if report.request.has('summary'):
            summary['result'] = f"SUMMARY: {report.request.get('summary')(report.result)}"
        else:
            summary['result'] = report.result
        if report.logspace and report.logpath:
            total_logs += 1
            summary['logpath_status'] = 'VALID' if report.logspace.exists(summary['logpath']) else 'MISSING'
            if summary['logpath_status'] == 'VALID':
                valid_logs += 1

        else:
            summary['logpath_status'] = None
        summary['logs_total'] = total_logs
        summary['logs_valid'] = valid_logs
        summary['logs_missing'] = total_logs - valid_logs

        return summary
    
    @DEPRECATED # use 'logpath_status', 'total_logs' and 'valid_logs' fields in summary/graph
    @staticmethod
    def validate_logs(report_or_summary, *, request_max_len=50):
        def _summary_validate_logs(summary, *, _validations, _valids=0, _invalids=0):
            logspace = _eval(summary['logspace']) if 'logspace' in summary else None
            validations = copy.copy(_validations)
            valids = _valids
            invalids = _invalids
            if logspace:
                logspace.filesystem.invalidate_cache() # TODO: do this to a clone of logspace
                if 'logpath' in summary and summary['logpath'] is not None:
                    valid = logspace.exists(summary['logpath'])
                    ivalid = 1 if valid else 0
                    valids += ivalid
                    invalids += (1-ivalid)
                    status = 'EXISTS' if valid else 'MISSING'
                    validation = {
                        'logpath': summary['logpath'],
                        'logspace': str(logspace),
                        'status': status,
                        'valid':  valid,
                        'request': f"{summary['request'][:request_max_len]}..."
                    }
                    validations.append(validation)
            if 'args_summaries' in summary:
                for arg_summary in summary['args_summaries']:
                    validations, valids, invalids = \
                        _summary_validate_logs(arg_summary, _validations=validations, _valids=valids, _invalids=invalids)
            if 'kwargs_summaries' in summary:
                for kwarg_summary in summary['kwargs_summaries']:
                    validations, valids, invalids = \
                        _summary_validate_logs(kwarg_summary, _validations=validations, _valids=valids, _invalids=invalids)
            return validations, valids, invalids
        if isinstance(report_or_summary, Report):
            summary = report_or_summary.summary
        else:
            summary = report_or_summary
        validations_, valids, invalids = _summary_validate_logs(summary, _validations=[])
        validations = {i: v for i, v in enumerate(validations_)}
        return {"VALID_LOGS": valids, "INVALID_LOGS": invalids, "LOGS": validations}

    def __repr__(self):
        summary = self.set(reset_use_summary=False).summary()
        summary_repr = repr(summary)
        return summary_repr

    def __str__(self):
        summary = self.summary()
        _ = str(summary)
        return _

    def logfile(self):
        try:
            _path = self.logpath
            _f = self.logspace.filesystem.open(_path, 'r')
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
    def _arg_summary(arg):
        if isinstance(arg, Report):
            summary = arg.summary()
        else:
            summary = {"result": arg}
        return summary

    @staticmethod
    def _arg_report(arg):
        if isinstance(arg, Response):
            report = arg.report()
        else:
            report = arg
        return report


class Response:
    def __init__(self,
                 request,
                 future,
                 *,
                 done_callback=None):
        self.request = request
        self.future = future
        self._done_callback = done_callback
        self.start_time = datetime.datetime.now()
        self.done_time = None
        self.future.add_done_callback(self.done_callback)

    @property
    def key(self):
        return self.request.task.key

    @property
    def id(self):
        return self.request.task.id

    @property
    def logspace(self):
        return self.request.task.logspace

    @property
    def logname(self):
        return self.request.task.logname

    def __str__(self):
        return signature.Tagger().str_ctor(self.__class__,
                                        self.request,
                                        self.future)

    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__,
                                         self.request,
                                         self.future)
                                
    def __tag__(self):
        return signature.Tagger().tag_ctor(self.__class__,
                                        self.request,
                                        self.future)
    def wait(self):
        try:
            self.result()
        finally:
            return self

    def report(self):
        report = Report(self)
        return report

    def result(self):
        result = self.future.result()
        if self.future.exception() is not None:
            raise self.future.exception().with_traceback(self.future.traceback())
        else:
            return result 

    def exception(self):
        return self.future.exception()

    def traceback(self):
        return self.future.traceback()
    
    @property
    def args_responses(self):
        return self.future.args_responses

    @property
    def kwargs_responses(self):
        return self.future.kwargs_responses

    @property
    def done(self):
        return self.future.done()

    @property
    def running(self):
        return self.future.running()

    @property
    def failed(self):
        return self.done and self.exception() is not None

    @property
    def succeeded(self):
        return self.done and self.exception() is None

    @property
    def pending(self):
        return not self.failed and not self.done and not self.running

    @property
    def status(self):
        if self.running:
            return 'Running'
        elif self.pending:
            return 'Pending'
        elif self.failed:
            return 'Failure'
        else:
            return 'Success'
        
    @property
    def logpath(self):
        if self.logname is None:
            return None
        logpath = self.logspace.join(self.logspace.path, self.logname)
        return logpath

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
    def done_callback(future):
        response = future.promise
        done_time = datetime.datetime.now()
        response._done = True
        response.done_time = done_time

        request = response.request
        stage = Task.Lifecycle.END
        if request.lifecycle_callback is not None:
            request.lifecycle_callback(stage, request, response)
        
        if response._done_callback is not None:
            response._done_callback(future)
            


"""
class Literal(Response):
    def __init__(self, request):
        super().__init__(request, request)

    def __tag__(self):
        return signature.Tagger().tag_ctor(Literal, self.request)

    def __repr__(self):
        return sigature.Tagger().repr_ctor(Literal, self.request)

    @property
    def done(self):
        return True
"""

class Closure(Response):
    def __init__(self, request):
        super().__init__(request, None)
        self._result = None

    def __tag__(self):
        return signature.Tagger().tag_ctor(Closure, self.request)

    def __repr__(self):
        return signature.Tagger().repr_ctor(Closure, self.request)

    def __str__(self):
        return signature.Tagger().repr_ctor(Closure, self.request)

    def result(self):
        if self._result is None:
            response = self.request.evaluate()
            self._result = response.result()
        return self._result

"""
#REMOVE 
class FIRST(Request):
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        repr = signature.Tagger().repr_ctor(self.__class__, *self.args)
        return repr

    def __str__(self):
        tag = signature.Tagger().str_ctor(self.__class__, *self.args)
        return tag

    def __tag__(self):
        tag = repr(self)
        return tag

    def apply(self, pool):
        _args = [arg.apply(pool) for arg in self.args]
        request = self.__class__(*_args)
        return request

    def evaluate(self):
        for arg in self.args:
            if isinstance(arg, Request):
                response = arg.evaluate()
            else:
                # FIX: ensure correctness of this Response() instantiation
                response = Response(None, arg)
            if response.exception() is None:
                return response
        response = Response(self, None, exc=ValueError(f"No success among request args {self.args}"))
        return response


class LAST(FIRST):
    def evaluate(self):
        for arg in self.args:
            if not isinstance(arg, Request):
                raise ValueError(f"Non-request arg: {args}")
            response = arg.evaluate()
        return response
"""


def AND(first, func, *args, **kwargs):
    # ignores `first`, but it gets evaluated before being fed in
    _ = func(*args, **kwargs)
    return _


def SECOND(first: Request, second: Request):
    request = second.redefine(AND, first, second.func, *second.args, **second.kwargs)
    return request


def LAST(head: Request, *tail: list[Request]):
    if len(tail) == 0:
        return head
    else:
        head_ = SECOND(head, tail[0])
        _ = LAST(head_, *tail[1:])
        return _
    

class BLOCK:
    # TODO: --> Requests
    class Stream:
        def __init__(self, iterable):
            self.iterable = iterable
            self._list = None

        @property
        def list(self):
            if self._list is None:
                self._list = list(self.iterable)
            return self._list

        @property
        def __str__(self):
            str = signature.Tagger().str_ctor(self.__class__, self.iterable)
            return str

        def __repr__(self):
            repr = signature.Tagger().repr_ctor(self.__class__, self.iterable)
            return repr

        def _tag_(self):
            _ = signature.Tagger().tag_ctor(self.__class__, self.iterable)
            return _

        def __len__(self):
            return len(self.list)

        def __getitem__(self, item):
            return self.list[item]

    class Request:
        def __init__(self, func, *args, **kwargs):
            self.func = func
            self.args = args
            self.kwargs = kwargs
            self._requests = None
            self.functors = []

        def __getitem__(self, item):
            return self.requests()[item]

        def __len__(self):
            return len(self.requests())

        def __iter__(self):
            return iter(self.requests())

        def _tag_(self):
            tag = signature.Tagger().tag_ctor(self.__class__,
                                            self.func,
                                            *self.args,
                                            **self.kwargs)
            return tag

        def __str__(self):
            tag = signature.Tagger().str_ctor(self.__class__,
                                        self.func,
                                        *self.args,
                                        **self.kwargs)
            return tag

        def __repr__(self):
            repr  = self._tag_()
            for functor in self.functors:
                repr = f"{repr}.apply({signature.Tagger().tag_object(functor)})"
            return repr

        def bind(self, *args, **kwargs):
            _request = Request(self.func, *args, **kwargs)
            request = _request
            for functor in self.functors:
                request = request.apply(functor)
            return request

        def _form_rstreams(self, rstreams=None):
            if rstreams is None:
                rstreams = []
            for i, arg in enumerate(self.args):
                if isinstance(arg, Stream):
                    if len(arg) < 1:
                        raise ValueError(f"arg with index {i} is a stream {arg} of len < 1: {len(arg)}")
                    if isinstance(arg.iterable, Requester):
                        rstreams = arg.iterable._form_rstreams(rstreams)
                    elif arg not in rstreams:
                        rstreams.insert(0, arg)
            for key, arg in self.kwargs.items():
                if isinstance(arg, Stream):
                    if len(arg) < 1:
                        raise ValueError(f"kwarg with key {key} is a stream {arg} of len < 1: {len(arg)}")
                    if isinstance(arg.iterable, BLOCK.Request):
                        rstreams = arg.iterable._form_rstreams(rstreams)
                    elif arg not in rstreams:
                        rstreams.insert(0, arg)
            return rstreams

        def _form_request(self, rscounters):
            args = [arg if not isinstance(arg, Stream) else
                    arg[rscounters[arg]] if not isinstance(arg.iterable, BLOCK.Request) else
                    arg.iterable._form_request(rscounters) for arg in self.args]
            kwargs = {key: arg if not isinstance(arg, Stream) else
                            arg[rscounters[arg]] if not isinstance(arg.iterable, BLOCK.Request) else
                            arg.iterable._form_request(rscounters) for key, arg in self.kwargs.items()}
            request = self.bind(*args, **kwargs)
            return request

        def _form_requests(self, rstreams=None):
            if rstreams is None:
                rstreams = self._form_rstreams()
            if len(rstreams) == 0:
                requests = [self.bind(*self.args, **self.kwargs)]
                return requests

            rscounters = {stream: 0 for stream in rstreams}

            requests = []
            carry = False
            while not carry:
                request = self._form_request(rscounters)
                requests.append(request)
                for arg in rscounters:
                    rscounters[arg] += 1
                    if rscounters[arg] < len(arg):
                        carry = False
                        break
                    else:
                        rscounters[arg] = 0
                        carry = True
            return requests

        def requests(self):
            if self._requests is None:
                self._requests = self._form_requests()
            return self._requests

        def evaluate(self):
            responses = [request.evaluate() for request in self.requests()]
            responder = BLOCK.Response(self, responses)
            return responder

        def reporter(self):
            responder = self.evaluate()
            reporter = responder.reporter()
            return reporter

        def compute(self):
            responder = self.evaluate()
            results = responder.results()
            return results

        def clone(self):
            requester = BLOCK.Request(self.func, *self.args, **self.kwargs)
            requester.functors = copy.deepcopy(self.functors)
            return requester

        def apply(self, functor):
            requester = self.clone()
            requester.functors.append(functor)
            return requester
        
    class Report:
        def __init__(self, responses):
            self.reports = [response.report() for response in responses]

        def __tag__(self):
            return signature.Tagger().tag_ctor(self.__class__, self.reports)

        def __repr__(self):
            return signature.Tagger().repr_ctor(self.__class__, self.reports)

        def __str__(self):
            return signature.Tagger().str_ctor(self.__class__, self.reports)

        def results(self):
            results = [report.result() for report in self.reports]
            return results

    class Response:
        def __init__(self, requester, responses):
            self.requester = requester
            self.responses = responses

        def __tag__(self):
            return signature.Tagger().tag_ctor(self.__class__, self.requester, self.responses)

        def __repr__(self):
            return signature.Tagger().repr_ctor(self.__class__, self.requester, self.responses)

        def __str__(self):
            return signature.Tagger().str_ctor(self.__class__, self.requester, self.responses)

        def reporter(self):
            reporter = BLOCK.Report(self.responses)
            return reporter

        def results(self):
            results = [response.result() for response in self.responses]
            return results


class Graph:
    def __init__(self, report_or_summary, indent=0, *, request_max_len=None, result_max_len=None, show=()):
        """
            show: tuple that can include 'logpath', 'exception', 'traceback'
        """
        if isinstance(report_or_summary, Report):
            summary = report_or_summary.summary
        else:
            summary = report_or_summary
        self.summary = summary
        if isinstance(self.summary, str):
            self.summary = _eval(self.summary)
        self.indent = indent
        self.request_max_len = request_max_len
        self.result_max_len = result_max_len
        self.show = show
        self.logspace = _eval(self.summary['logspace']) if 'logspace' in self.summary else None

    def clone(self):
        clone = self.__class__(self.summary, self.indent, request_max_len=self.request_max_len, result_max_len=self.result_max_len, show=self.show)
        return clone

    @property
    def request(self):
        request = self.summary['request'] if 'request' in self.summary else None
        requeststr = truncate_str(str(request), self.request_max_len, use_ellipsis=True) if request else None
        return requeststr

    @property
    def result(self):
        result = self.summary['result'] if 'result' in self.summary else None
        resultstr = truncate_str(str(result), self.result_max_len, use_ellipsis=True) if result else None
        return resultstr

    @property
    def args(self):
        _args = None
        if 'args_summaries' in self.summary:
            _args = []
            for arg in self.summary['args_summaries']:
                _arg = self.__class__(arg, self.indent+1, request_max_len=self.request_max_len, result_max_len=self.result_max_len, show=self.show)
                _args.append(_arg)
        return _args

    @property
    def kwargs(self):
        _kwargs = None
        if 'kwargs_summaries' in self.summary:
            _kwargs = {}
            for key, kwarg in self.summary['kwargs_summaries'].items():
                _kwarg = self.__class__(kwarg, self.indent+1, request_max_len=self.request_max_len, result_max_len=self.result_max_len, show=self.show)
                _kwargs[key] = _kwarg
        return _kwargs
    
    def print(self):
        print(self)

    @property
    def simple(self):
        simple = not self.request and not self.args and not self.kwargs
        return simple

    def with_request_max_len(self, request_max_len):
        graph = self.clone()
        graph.request_max_len = request_max_len
        return graph

    def with_result_max_len(self, result_max_len):
        graph = self.clone()
        graph.result_max_len = result_max_len
        return graph

    def with_show(self, *show):
        graph = self.clone()
        graph.show = show
        return graph

    def with_indent(self, indent):
        graph = self.clone()
        graph.indent = indent
        return graph

    def __getitem__(self, item):
        if isinstance(item, int):
            if item >= 0 and item < len(self.args):
                return self.args[item]
            else:
                return None
        elif isinstance(item, str):
            if item in self.kwargs:
                return self.kwargs[item]
            else:
                return None
        else:
            raise ValueError("No item {item}")

    def __str__(self):
        prefix = '\t'*self.indent
        if self.simple:
            s = f"{self.result}"
        else:
            s = \
                f"{prefix}request: {self.request}" + \
                f"\n{prefix}result:  {self.result}"
            for attr in self.show:
                s = s + f"\n{prefix}{attr}: {self.summary[attr]}"
            for i, arg in enumerate(self.args):
                if arg.simple:
                    s = s + f"\n{prefix}args[{i}]: " + f"{arg}"
                else:
                    s = s + f"\n{prefix}args[{i}]:\n" + f"{arg}"

            for key, kwarg in self.kwargs.items():
                if kwarg.simple:
                    s = s + f"\n{prefix}kwargs[{key}]: " + f"{kwarg}"
                else:
                    s = s + f"\n{prefix}kwargs[{key}]:\n" + f"{kwarg}"
        return s
    
    def __getattr__(self, attrname):
        attr = self.summary[attrname]
        return attr

    def node(self, *links):
        g = self
        for link in links:
            g = g[link]
        g_ = g.with_indent(0)
        return g_

    def log(self, dataspace=None):
        if dataspace is None:
            dataspace = self.logspace
        if dataspace is None:
            return None
        file = dataspace.filesystem.open(self.summary['logpath'], 'r')
        log = ''.join(file.readlines())
        return log
    
    def validate_logs(self, *, request_max_len=50):
        _ = Report.validate_logs(self.summary, request_max_len=request_max_len)
        return _


@DEPRECATED
@ALIAS
class ReportSummaryGraph(Graph):
    pass




@ALIAS
def report_summary_graph(*args, **kwargs):
    return report_graph(*args, **kwargs) 


@DEPRECATED
def report_summary_truncate(summary, *, result_max_len=50, request_max_len=50):
    if isinstance(summary, dict):
        summary_ = {}
        for key, val in summary.items():
            if key == 'result':
                resultstr = str(val)
                summary_['result'] = f"\"\"\"{truncate_str(resultstr, result_max_len, use_ellipsis=True)}...\"\"\""
            elif key == 'request':
                requeststr = str(val)
                summary_['request'] = f"\"\"\"{truncate_str(requeststr, request_max_len, use_ellipsis=True)}...\"\"\""
            else:
                summary_[key] = copy.copy(val)
            if key == 'args_summaries':
                summary_['args_summaries'] = []
                for arg_summary in val:
                    arg_summary_ = report_summary_truncate(arg_summary, request_max_len=request_max_len, result_max_len=result_max_len)
                    summary_['args_summaries'].append(arg_summary_)
            if key == 'kwargs_summaries':
                summary_['kwargs_summaries'] = {}
                for kwarg, kwarg_summary in val.items():
                    kwarg_summary_ = report_summary_truncate(kwarg_summary, request_max_len=request_max_len, result_max_len=result_max_len)
                    summary_['kwargs_summaries'][kwarg] = kwarg_summary_

    else:
        summary_ = summary
    return summary_
