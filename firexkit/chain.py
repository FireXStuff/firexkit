from celery.local import PromiseProxy
from functools import wraps


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
