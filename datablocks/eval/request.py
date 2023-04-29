import collections
import copy
import datetime
import enum
import importlib
import inspect
import logging
import time

from .. import utils
from .. import signature
from ..utils import REPLACE, REMOVE, DEPRECATED, ALIAS, truncate_str

_eval = __builtins__['eval']

logger = logging.getLogger(__name__)


class Task:
    class Lifecycle(enum.IntEnum):
        ERROR = -1
        BEGIN = 0
        END = 1

    def __call__(self, *args, **kwargs):
        pass

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
        if isinstance(func, str):
            self.func = URL_RPC(func)
        else:
            self.func = func
        if not callable(self.func):
            raise ValueError(f"Request func {self.func} of non-callable type {type(self.func)}")
        self.args, self.kwargs = args, kwargs
        self.throw = False
        self.lifecycle_callback = None

    def with_lifecycle_callback(self, lifecycle_callback):
        """
            lifecycle_callback: (Task.Lifecycle, request, Option[response] -> None)
        """
        self.lifecycle_callback = lifecycle_callback
        return self

    def with_throw(self, throw=True):
        request = copy.copy(self)
        request.throw = throw
        return request

    @ALIAS
    def with_evaluate_raises_exceptions(self, raises=True):
        request = self.with_throw(raises)
        return request

    @property
    def evaluate_raises_exceptions(self):
        return self.throw

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.func == other.func and \
               self.args == other.args and self.kwargs == other.kwargs

    def rebind(self, *args, **kwargs):
        request = Request(self.func, *args, **kwargs)\
                    .with_evaluate_raises_exceptions(self.evaluate_raises_exceptions)\
                    .with_throw(self.throw)\
                    .with_lifecycle_callback(self.lifecycle_callback)
        return request

    def __str__(self):
        str = signature.Tagger().str_func(self.func, *self.args, **self.kwargs)
        return str

    def __repr__(self):
        repr = signature.Tagger().repr_func(self.func, *self.args, **self.kwargs)
        return repr

    def __tag__(self):
        _ = signature.Tagger().tag_func(self.func, *self.args, **self.kwargs)
        return _

    def iargs_kargs_kwargs(self):
        if hasattr(self.func, '__iargs_kargs_kwargs__'):
            return self.func.__iargs_kargs_kwargs__
        iargs, kargs, kwargs = \
          signature.func_iargs_kargs_kwargs(self.func,
                                        False,
                                        True,
                                        *self.args,
                                        **self.kwargs)
        return iargs, kargs, kwargs

    def compute_args_kwargs(self, reports, args, kwargs, **task_trace):
        _args = [self._compute_arg(reports, a, i, **task_trace) for i, a in enumerate(args)]
        _kwargs = {k: self._compute_kwarg(reports, a, k, **task_trace) for k, a in kwargs.items()}
        return _args, _kwargs

    def _compute_arg(self, report, arg, index, **task_trace):
        '''
            logger.debug(f"Computing args[{index}] for request tagged\n\t{self.tag}\n"
                        f"\t\targs[{index}] {arg}")
                    '''
        r = self._compute_argument(report, arg, **task_trace)
        '''logger.debug(f"\n\t\tDone with args[{index}]")'''
        return r

    def _compute_kwarg(self, report, kwarg, key, **task_trace):
        '''
            logger.debug(f"Computing kwargs[{key}] for request tagged\n\t{self.tag}\n"
                        f"\t\tkwargs[{key}]  {kwarg}")
                    '''
        r = self._compute_argument(report, kwarg, **task_trace)
        '''logger.debug(f"\n\t\tDone with kwargs[{key}]")'''
        return r

    # TODO: eliminate 'report' option?  It doesn't seem to be use5
    def _compute_argument(self, report, arg, **task_trace):
        if report:
            report = arg.reporter() if isinstance(arg, Responder) else \
                         arg.report() if isinstance(arg, Response) else \
                         arg.reporter() if isinstance(arg, Requester) else \
                         arg.report() if isinstance(arg, Request) else \
                         arg
            return report
        else:
            response = \
                        arg.evaluate(**task_trace) if isinstance(arg, Requester) else \
                        arg.evaluate(**task_trace) if isinstance(arg, Request) else \
                        arg
            return response

    def _arg_result(self, arg):
        if not isinstance(arg, Report) and not isinstance(arg, Response):
            result = arg
        else:
            exception = arg.exception()
            if exception is not None:
                raise exception
            result = arg.result()
        return result

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

    def compute(self, **task_trace):
        response = self.evaluate(**task_trace)
        result = response.result()
        return result

    def evaluate(self, **task_trace):
        ev = None
        args_responses = []
        kwargs_responses = {}
        try:
            compute_reports = True
            compute_responses = not compute_reports
            args_responses, kwargs_responses = self.compute_args_kwargs(compute_responses, self.args, self.kwargs, **task_trace)
            _args = [self._arg_result(arg) for arg in args_responses]
            _kwargs = {key: self._arg_result(arg) for key, arg in kwargs_responses.items()}
            r = self.func(*_args, **_kwargs)
        except Exception as e:
            exc_type, exc_value, exc_traceback = utils.exc_info()
            if self.evaluate_raises_exceptions:
                raise e
            logger.debug(f"Caught exception {repr(str(e))}")
            ev = Response(request=self,
                          result=None,
                          args_responses=args_responses,
                          kwargs_responses=kwargs_responses,
                          exception=exc_value,
                          traceback=exc_traceback,
                          **task_trace)
            return ev
        if ev is None:
            ev = Response(request=self,
                          result=r,
                          exception=None,
                          args_responses=args_responses,
                          kwargs_responses=kwargs_responses, 
                          **task_trace)
        return ev

    def submit(self, functor, **task_trace):
        p = self.apply(functor).evaluate(**task_trace)
        return p

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

# TODO: --> Block.Request
class Requester:
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
                if isinstance(arg.iterable, Requester):
                    rstreams = arg.iterable._form_rstreams(rstreams)
                elif arg not in rstreams:
                    rstreams.insert(0, arg)
        return rstreams

    def _form_request(self, rscounters):
        args = [arg if not isinstance(arg, Stream) else
                arg[rscounters[arg]] if not isinstance(arg.iterable, Requester) else
                arg.iterable._form_request(rscounters) for arg in self.args]
        kwargs = {key: arg if not isinstance(arg, Stream) else
                         arg[rscounters[arg]] if not isinstance(arg.iterable, Requester) else
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

    def evaluate(self, **task_trace):
        responses = [request.evaluate(**task_trace) for request in self.requests()]
        responder = Responder(self, responses)
        return responder

    def reporter(self, **task_trace):
        responder = self.evaluate(**task_trace)
        reporter = responder.reporter()
        return reporter

    def compute(self, **task_trace):
        responder = self.evaluate(**task_trace)
        results = responder.results()
        return results

    def clone(self):
        requester = Requester(self.func, *self.args, **self.kwargs)
        requester.functors = copy.deepcopy(self.functors)
        return requester

    def apply(self, functor):
        requester = self.clone()
        requester.functors.append(functor)
        return requester


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

    def __repr__(self):
        summary = self.summary()
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

    def summary(self):
        tagger = signature.Tagger()
        report = self
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
                       result=report.result,
                       args_summaries=[report._arg_summary(r) for r in report.args_reports],
                       kwargs_summaries={k: report._arg_summary(v) for k, v in report.kwargs_reports.items()},
                       request=tagger.str_object(report.request))
        return summary

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
                 result,
                 exception,
                 traceback=None,
                 args_responses=None,
                 kwargs_responses=None,
                 task_key=None,
                 task_id=None,
                 task_logspace=None,
                 task_logname=None):
        self.request = request
        self._result = result
        self._exception = exception
        self._traceback = traceback
        self.args_responses = args_responses
        self.kwargs_responses = kwargs_responses
        self.start_time = self.done_time = datetime.datetime.now()
        self.key = task_key
        self.id = task_id
        self.logspace = task_logspace
        self.logname = task_logname

    def __str__(self):
        return signature.Tagger().str_ctor(self.__class__,
                                        self.request,
                                        self._result,
                                        self.exception(),
                                        None,
                                        None)

    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__,
                                         self.request,
                                         self._result,
                                         self.exception(),
                                         None,
                                         None)

    def __tag__(self):
        return signature.Tagger().tag_ctor(self.__class__,
                                        self.request,
                                        self._result,
                                        self.exception(),
                                        None,
                                        None)
    @property
    def request_id(self):
        return None

    def wait(self):
        try:
            self.result()
        finally:
            return self

    def report(self):
        report = Report(self)
        return report

    def result(self):
        if self._exception is not None:
            raise self._exception.with_traceback(self._traceback)
        return self._result

    def exception(self):
        return self._exception

    def traceback(self):
        return self._traceback

    @property
    def done(self):
        return True

    @property
    def running(self):
        return False

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

    def result(self, **task_trace):
        if self._result is None:
            response = self.request.evaluate(**task_trace)
            self._result = response.result()
        return self._result


class Reporter:
    def __init__(self, responses):
        self.reports = [response.report() for response in responses]

    def __tag__(self):
        return signature.Tagger().tag_ctor(self.__class__, self.reports)

    def __repr__(self):
        return signature.Tagger().repr_ctor(self.__class__, self.reports)

    def __str__(self):
        return signature.Tagger().str_ctor(self.__class__, self.reports)

    def results(self, **task_trace):
        results = [report.result(**task_trace) for report in self.reports]
        return results


class Responder:
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
        reporter = Reporter(self.responses)
        return reporter

    def results(self, **task_trace):
        results = [response.result(**task_trace) for response in self.responses]
        return results


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

    def evaluate(self, **task_trace):
        for arg in self.args:
            if isinstance(arg, Request):
                response = arg.evaluate(**task_trace)
            else:
                response = Response(None, arg)
            if response.exception() is None:
                return response
        response = Response(self, None, exc=ValueError(f"No success among request args {self.args}"))
        return response


class LAST(FIRST):
    def evaluate(self, **task_trace):
        for arg in self.args:
            if isinstance(arg, Request):
                response = arg.evaluate(**task_trace)
            else:
                response = Response(None, arg)
            if response.exception() is not None:
                return response
        return response


@ALIAS
class FirstSuccessRequest(FIRST):
    pass


class ReportSummaryGraph:
    def __init__(self, summary, indent=0, *, request_max_len=50, result_max_len=50, print=(), logspace=None):
        """
            print: tuple that can include 'logpath', 'exception', 'traceback'
        """
        self.summary = summary
        if isinstance(self.summary, str):
            self.summary = _eval(self.summary)
        self.indent = indent
        self.request_max_len = request_max_len
        self.result_max_len = result_max_len
        self.print = print
        self.logspace = logspace

    def clone(self):
        clone = self.__class__(self.summary, self.indent, request_max_len=self.request_max_len, result_max_len=self.result_max_len, print=self.print)
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
                _arg = self.__class__(arg, self.indent+1, request_max_len=self.request_max_len, result_max_len=self.result_max_len, print=self.print)
                _args.append(_arg)
        return _args

    @property
    def kwargs(self):
        _kwargs = None
        if 'kwargs_summaries' in self.summary:
            _kwargs = {}
            for key, kwarg in self.summary['kwargs_summaries'].items():
                _kwarg = self.__class__(kwarg, self.indent+1, request_max_len=self.request_max_len, result_max_len=self.result_max_len, print=self.print)
                _kwargs[key] = _kwarg
        return _kwargs

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

    def with_print(self, *print):
        graph = self.clone()
        graph.print = print
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
            for attr in self.print:
                s = s + f"\n{prefix}{attr}: {self.summary[attr]}"
            for i, arg in enumerate(self.args):
                if arg.simple:
                    s = s + f"\n{prefix}[{i}]: " + f"{arg}"
                else:
                    s = s + f"\n{prefix}[{i}]:\n" + f"{arg}"

            for key, kwarg in self.kwargs.items():
                if kwarg.simple:
                    s = s + f"\n{prefix}[{key}]: " + f"{kwarg}"
                else:
                    s = s + f"\n{prefix}[{key}]:\n" + f"{kwarg}"
        return s

    def node(self, *links):
        g = self
        for link in links:
            g = g[link]
        g_ = g.with_indent(0)
        return g_

    def logpath(self):
        return self.summary['logpath']

    def log(self, dataspace=None):
        if dataspace is None:
            dataspace = self.logspace
        file = dataspace.filesystem.open(self.summary['logpath'], 'r')
        log = ''.join(file.readlines())
        return log
    
    def validate_logs(self, *, dataspace=None, request_max_len=50):
        if dataspace is None:
            dataspace = self.logspace
        _ = report_summary_validate_logs(self, dataspace=dataspace, request_max_len=request_max_len)
        return _


def report_summary_validate_logs(summary, *, dataspace, request_max_len=50):
    def _summary_validate_logs(summary, *, dataspace, _validations, _valids=0, _invalids=0):
        validations = copy.copy(_validations)
        valids = _valids
        invalids = _invalids
        if 'logpath' in summary and summary['logpath'] is not None:
            valid = dataspace.exists(summary['logpath'])
            ivalid = 1 if valid else 0
            valids += ivalid
            invalids += (1-ivalid)
            status = 'EXISTS' if valid else 'MISSING'
            validation = {
                'logpath': summary['logpath'],
                'dataspace': str(dataspace),
                'status': status,
                'valid':  valid,
                'request': f"{summary['request'][:request_max_len]}..."
            }
            validations.append(validation)
        if 'args_summaries' in summary:
            for arg_summary in summary['args_summaries']:
                validations, valids, invalids = \
                    _summary_validate_logs(arg_summary, dataspace=dataspace, _validations=validations, _valids=valids, _invalids=invalids)
        if 'kwargs_summaries' in summary:
            for kwarg_summary in summary['kwargs_summaries']:
                validations, valids, invalids = \
                    _summary_validate_logs(kwarg_summary, dataspace=dataspace, _validations=validations, _valids=valids, _invalids=invalids)
        return validations, valids, invalids
    dataspace.filesystem.invalidate_cache() # TODO: do this to a clone of dataspace
    validations_, valids, invalids = _summary_validate_logs(summary, dataspace=dataspace, _validations=[])
    validations = {i: v for i, v in enumerate(validations_)}
    return {"VALID_LOGS": valids, "INVALID_LOGS": invalids, "LOGS": validations}

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
