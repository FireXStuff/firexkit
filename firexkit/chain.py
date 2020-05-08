import inspect
from inspect import signature, getfullargspec
from functools import wraps
from typing import Union

from celery.canvas import chain, Signature
from celery.local import PromiseProxy
from celery.result import AsyncResult
from celery.utils.log import get_task_logger

from firexkit.result import wait_on_async_results_and_maybe_raise
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.task import parse_signature, FireXTask, undecorate, ReturnsCodingException, get_attr_unwrapped

logger = get_task_logger(__name__)


def returns(*args):
    """ The decorator is used to allow us to specify the keys of the
        dict that the task returns.

        This is used only to signal to the user the inputs and outputs
        of a task, and deduce what arguments are required for a chain.
    """
    if not args or len(args) != len(set(args)):
        raise ReturnsCodingException("@returns cannot contain duplicate keys")

    def decorator(func):
        if type(func) is PromiseProxy:
            raise ReturnsCodingException("@returns must be applied to a function (before @app.task)")

        # Store the arguments of the decorator as a function attribute
        undecorated = func
        while hasattr(undecorated, '__wrapped__'):
            undecorated = undecorated.__wrapped__
        undecorated._decorated_return_keys = args

        # Return a dictionary mapping the tuple returned by the original function
        # to the keys specified by the arguments to the @returns decorator
        @wraps(func)
        def returns_wrapper(*orig_args, **orig_kwargs) -> dict:
            result = func(*orig_args, **orig_kwargs)
            return FireXTask.convert_returns_to_dict(undecorated._decorated_return_keys, result)

        return returns_wrapper

    return decorator


class InjectArgs(object):
    def __init__(self, **kwargs):
        self.injectArgs = kwargs

    def __or__(self, other) -> Union[chain, Signature]:
        if isinstance(other, InjectArgs):
            self.injectArgs.update(other.injectArgs)
        else:
            # chains and signatures are both handled by this
            _inject_args_into_signature(other, **self.injectArgs)
        return other

    def __ior__(self, other: Union[chain, Signature]) -> Union[chain, Signature]:
        return self | other


def _inject_args_into_signature(sig, **kwargs):
    try:
        # This might be a chain
        task = sig.tasks[0]
    except AttributeError:
        # This might be a Task
        task = sig

    existing = task.kwargs.keys()
    subset = {k: v for k, v in kwargs.items() if k not in existing}
    task.kwargs.update(subset)


Signature.injectArgs = _inject_args_into_signature


def verify_chain_arguments(sig: Signature):
    """
    Verifies that the chain is not missing any parameters. Asserts if any parameters are missing, or if a
    reference parameter (@something) has not provider
    """
    try:
        tasks = sig.tasks
    except AttributeError:
        tasks = [sig]

    missing = {}
    previous = set()
    ref_args = {}
    undefined_indirect = {}
    for task in tasks:
        task_obj = sig.app.tasks[task.task]
        partial_bound = set(signature(task_obj.run).bind_partial(*task.args).arguments.keys())
        kwargs_keys = set(task.kwargs.keys()) | {'args', 'kwargs'}

        if isinstance(task_obj, FireXTask):
            required_args = task_obj.required_args
        else:
            required_args, _ = parse_signature(inspect.signature(task_obj.run))

        missing_params = set(required_args) - (partial_bound | kwargs_keys | previous)
        if missing_params:
            missing[task_obj.name] = missing_params

        previous |= (partial_bound | kwargs_keys)
        # get @returns for next loop
        try:
            current_task_returns = set(task_obj.return_keys)
            previous |= current_task_returns
        except AttributeError:
            try:
                current_task_returns = set(get_attr_unwrapped(task_obj, '_decorated_return_keys'))
                previous |= current_task_returns
            except AttributeError:
                current_task_returns = set()

        # If any of the previous keys has a dynamic return, then we can't do any validation
        if any(FireXTask.is_dynamic_return(k) for k in current_task_returns):
            break

        # check for validity of reference values (@ arguments) that are consumed by this microservice
        necessary_args = getfullargspec(undecorate(task_obj)).args
        new_ref = {k: v[1:] for k, v in task.kwargs.items() if
                   hasattr(v, 'startswith') and v.startswith(BagOfGoodies.INDIRECT_ARG_CHAR)}
        ref_args.update(new_ref)
        for needed in necessary_args:
            if needed in ref_args and ref_args[needed] not in previous:
                undefined_indirect[needed] = ref_args[needed]

    if missing:
        txt = ''
        for k, v in missing.items():
            for arg in v:
                if txt:
                    txt += '\n'
                service_path = k.split('.')
                txt += ' ' + arg + '\t: required by "%s" (%s)' % \
                       (service_path[-1], '.'.join(service_path[0:-1]))
        raise InvalidChainArgsException('Missing mandatory arguments: \n%s' % txt, missing)
    if undefined_indirect:
        txt = "\n".join([k + ": " + v for k, v in undefined_indirect.items()])
        raise InvalidChainArgsException('Chain indirectly references the following unavailable parameters: \n%s' %
                                        txt, undefined_indirect)
    return True


class InvalidChainArgsException(Exception):
    def __init__(self, msg, wrong_args: dict = None):
        super(InvalidChainArgsException, self).__init__(msg)
        self.wrong_args = wrong_args if wrong_args else {}


def _enqueue(self: Signature,
             block: bool = False,
             raise_exception_on_failure: bool = True,
             caller_task: Signature = None,
             queue: str = None,
             priority: int = None,
             soft_time_limit: int = None) -> AsyncResult:

    verify_chain_arguments(self)

    if queue:
        self.set_queue(queue)

    if priority:
        self.set_priority(priority)

    if soft_time_limit:
        self.soft_time_limit(soft_time_limit)

    result_promise = self.delay()
    if block:
        wait_on_async_results_and_maybe_raise(results=result_promise,
                                              raise_exception_on_failure=raise_exception_on_failure,
                                              caller_task=caller_task)
    return result_promise


def set_execution_options(sig: Signature, **options):
    """Set arbitrary executions options in every task in the :attr:`sig`"""
    try:
        [task.set(**options) for task in sig.tasks]
    except AttributeError:
        sig.set(**options)


def set_priority(sig: Signature, priority: int):
    """Set the :attr:`priority` execution option in every task in :attr:`sig`"""
    set_execution_options(sig, priority=priority)


def set_queue(sig: Signature, queue):
    """Set the :attr:`queue` execution option in every task in :attr:`sig`"""
    set_execution_options(sig, queue=queue)


def set_soft_time_limit(sig: Signature, soft_time_limit):
    """Set the :attr:`soft_time_limit` execution option in every task in :attr:`sig`"""
    set_execution_options(sig, soft_time_limit=soft_time_limit)


def set_label(sig: Signature, label):
    sig.set(label=label)


def get_label(sig: Signature):
    try:
        return sig.options['label']
    except KeyError:
        try:
            return '|'.join([task.name for task in sig.tasks])
        except AttributeError:
            return sig.name


Signature.set_execution_options = set_execution_options
Signature.set_priority = set_priority
Signature.set_queue = set_queue
Signature.set_soft_time_limit = set_soft_time_limit
Signature.set_label = set_label
Signature.get_label = get_label
Signature.enqueue = _enqueue
