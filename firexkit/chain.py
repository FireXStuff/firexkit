import inspect
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.task import parse_signature, FireXTask, get_attr_unwrapped, undecorate
from functools import wraps
from celery.canvas import chain
from celery.local import PromiseProxy


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


class ReturnsCodingException(BaseException):
    pass


def verify_chain_arguments(chain_of_tasks: chain):
    """
    Verifies that the chain is not missing any parameters. Asserts if any parameters are missing, or if a
    reference parameter (@something) has not provider
    """
    missing = {}
    previous = set()
    ref_args = {}
    undefined_indirect = {}
    for task in chain_of_tasks.tasks:
        task_obj = task.app.tasks[task.task]
        partial_bound = set(inspect.signature(task_obj.run).bind_partial(*task.args).arguments.keys())
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
        necessary_args = inspect.getfullargspec(undecorate(task_obj)).args
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


class InvalidChainArgsException(BaseException):
    def __init__(self, msg, wrong_args):
        super(InvalidChainArgsException, self).__init__(msg)
        self.wrong_args = wrong_args
