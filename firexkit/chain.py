from inspect import signature, getfullargspec
from functools import wraps
from typing import Union

from celery.canvas import chain, Signature
from celery.local import PromiseProxy
from celery.utils.log import get_task_logger

from firexkit.result import ChainInterruptedException, wait_on_async_results, get_result_logging_name
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.task import parse_signature, FireXTask, get_attr_unwrapped, undecorate


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
        undecorated._out = set(args)
        undecorated._return_keys = args

        # Return a dictionary mapping the tuple returned by the original function
        # to the keys specified by the arguments to the @returns decorator
        @wraps(func)
        def returns_wrapper(*orig_args, **orig_kwargs)->dict:
            result = func(*orig_args, **orig_kwargs)
            if type(result) != tuple and isinstance(result, tuple):
                # handle named tuples, they are a result, not all the results
                result = (result,)

            no_expected_keys = len(args)
            if isinstance(result, tuple):
                if len(result) != no_expected_keys:
                    raise ReturnsCodingException('Expected %d keys in @returns, got %d' %
                                                 (no_expected_keys, len(result)))
                result = dict(zip(args, result))
            else:
                if no_expected_keys != 1:
                    raise ReturnsCodingException('Expected one key in @returns')
                result = {args[0]: result}
            return result

        return returns_wrapper

    return decorator


class ReturnsCodingException(Exception):
    pass


class InjectArgs(object):
    def __init__(self, **kwargs):
        self.injectArgs = kwargs

    def __or__(self, other)->Union[chain, Signature]:
        if isinstance(other, InjectArgs):
            self.injectArgs.update(other.injectArgs)
        else:
            # chains and signatures are both handled by this
            _inject_args_into_signature(other, **self.injectArgs)
        return other

    def __ior__(self, other: Union[chain, Signature])->Union[chain, Signature]:
        return self | other


def _inject_args_into_signature(sig, **kwargs):
    try:
        # This might be a chain
        sig.tasks[0].kwargs.update(**kwargs)
    except AttributeError:
        # This might be a Task
        sig.kwargs.update(**kwargs)


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
            required_args, _ = parse_signature(task_obj)

        missing_params = set(required_args) - (partial_bound | kwargs_keys | previous)
        if missing_params:
            missing[task_obj.name] = missing_params

        previous |= (partial_bound | kwargs_keys)
        # get @returns for next loop
        try:
            previous |= get_attr_unwrapped(task_obj, '_out')
        except AttributeError:
            pass

        # check for validity of reference values (@ arguments) that are consumed by this microservice
        necessary_args = getfullargspec(undecorate(task_obj)).args
        new_ref = {k: v[1:] for k, v in task.kwargs.items() if
                   hasattr(v, 'startswith') and v.startswith(BagOfGoodies.INDIRECT_ARG_CHAR)}
        ref_args.update(new_ref)
        for needed in necessary_args:
            if needed in ref_args and ref_args[needed] not in previous:
                undefined_indirect[needed] = ref_args[needed]

    if missing:
        txt = "\n".join([k + ": " + ",".join(v) for k, v in missing.items()])
        raise InvalidChainArgsException('Chain missing the following parameters: \n%s' % txt, missing)
    if undefined_indirect:
        txt = "\n".join([k + ": " + v for k, v in undefined_indirect.items()])
        raise InvalidChainArgsException('Chain indirectly references the following unavailable parameters: \n%s' %
                                        txt, undefined_indirect)
    return True


class InvalidChainArgsException(Exception):
    def __init__(self, msg, wrong_args: dict=None):
        super(InvalidChainArgsException, self).__init__(msg)
        self.wrong_args = wrong_args if wrong_args else {}


def _enqueue(self, block=False, raise_exception_on_failure=True, caller_task=None):
    verify_chain_arguments(self)
    result = self.delay()
    if block:
        try:
            wait_on_async_results(result, caller_task=caller_task)
            logger.debug(get_result_logging_name(result) + ' completed. Unblocking.')
            if result.failed():
                raise ChainInterruptedException(get_result_logging_name(result))
        except ChainInterruptedException:
            if raise_exception_on_failure:
                raise

    return result


def set_attr(sig: Signature, **options):
    try:
        [task.set(**options) for task in sig.tasks]
    except AttributeError:
        sig.set(**options)


def set_priority(sig: Signature, priority):
    set_attr(sig, priority=priority)


def set_queue(sig: Signature, queue):
    set_attr(sig, queue=queue)


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


Signature.set_priority = set_priority
Signature.set_queue = set_queue
Signature.set_label = set_label
Signature.get_label = get_label
Signature.enqueue = _enqueue
