import time
from collections import namedtuple

import traceback
from typing import Union

from celery.result import AsyncResult
from celery.signals import task_prerun
from celery.states import FAILURE, REVOKED, SUCCESS
from celery.utils.log import get_task_logger
from firexkit.revoke import RevokedRequests

RETURN_KEYS_KEY = '__task_return_keys'

logger = get_task_logger(__name__)


def get_task_name_from_result(result):
    try:
        backend = result.app.backend
    except AttributeError:
        from celery import current_app
        backend = current_app.backend
    name = backend.get(str(result))
    if name is None:
        name = ''
    else:
        name = name.decode()
    return name


def get_tasks_names_from_results(results):
    return [get_result_logging_name(r) for r in results]


def get_result_logging_name(result: AsyncResult, name=None):
    if name is None:
        name = get_task_name_from_result(result)
    return '%s[%s]' % (name, result)


# noinspection PyUnusedLocal
@task_prerun.connect
def populate_task_name(task_id, task, args, kwargs, **donotcare):
    from celery import current_app
    current_app.backend.set(task_id, task.name)


def is_result_ready(result: AsyncResult, timeout=None, retry_delay=0.1):
    """
    Protect against broker being temporary unreachable and throwing a TimeoutError
    """
    timeout_time = time.time() + timeout if timeout else None

    while True:
        try:
            return result.ready()
        except Exception as e:
            # need to handle different timeout exceptions from different brokers
            if type(e).__name__ != "TimeoutError":
                raise
            if not timeout:
                logger.warning('Backend was not reachable and timed out...retrying')
                time.sleep(retry_delay)
            else:
                if time.time() < timeout_time:
                    logger.warning('Backend was not reachable and timed out...retrying for max %r seconds' % timeout)
                    time.sleep(retry_delay)
                else:
                    logger.error('Reached max timeout of %r...giving up!' % timeout)
                    raise


def find_all_unsuccessful(result: AsyncResult, ignore_non_ready=False, depth=0)->{}:
    name = get_result_logging_name(result)
    state_str = '-'*depth*2 + '->%s: ' % name

    failures = {}
    if is_result_ready(result):
        # Did this task fail?
        state = result.state
        logger.debug(state_str + state)
        if state == FAILURE:
            failures[result.id] = name
    else:
        # This task was not ready
        logger.debug(state_str + 'NOT READY!')
        if not ignore_non_ready:
            failures[result.id] = name

    # Look for failures in the children
    children = result.children
    if children:
        depth += 1
        for child in children:
            failures.update(find_all_unsuccessful(child, ignore_non_ready, depth))
    return failures


def find_unsuccessful_in_chain(result: AsyncResult)->{}:
    failures = []
    did_not_run = []
    node = result
    while node:
        if is_result_ready(node):
            # Did this task fail?
            if node.state == FAILURE:
                failures.append(node)
        else:
            # This task was not ready
            did_not_run.append(node)
        node = node.parent
    # Should reverse the items since we're traversing the chain from RTL
    failures.reverse()
    did_not_run.reverse()
    res = {}
    if failures:
        res['failed'] = failures
    if did_not_run:
        res['not_run'] = did_not_run
    return res


def _check_for_traceback_in_parents(result):
    parent = result.parent
    if parent:
        if parent.failed():
            cause = parent.result if isinstance(parent.result, Exception) else None
            raise ChainInterruptedException(task_id=str(parent),
                                            task_name=get_task_name_from_result(parent),
                                            cause=cause)
        elif parent.state == REVOKED:
            raise ChainRevokedException(task_id=str(parent),
                                        task_name=get_task_name_from_result(parent))
        else:
            return _check_for_traceback_in_parents(parent)


WaitLoopCallBack = namedtuple('WaitLoopCallBack', ['func', 'frequency', 'kwargs'])


def send_block_task_states_to_caller_task(func):
    def wrapper(*args, **kwargs):
        caller_task = kwargs.pop("caller_task", None)
        if caller_task and not caller_task.request.called_directly:
            caller_task.send_event('task-blocked')
        try:
            func(*args, **kwargs)
        finally:
            if caller_task and not caller_task.request.called_directly:
                caller_task.send_event('task-unblocked')
    return wrapper


@send_block_task_states_to_caller_task
def wait_on_async_results(results,
                          max_wait=None,
                          callbacks: [WaitLoopCallBack]=tuple(),
                          sleep_between_iterations=0.05):
    if not results:
        return

    max_trials = max_wait/sleep_between_iterations if max_wait else None
    trials = 0

    if isinstance(results, AsyncResult):
        results = [results]

    failures = []
    for result in results:
        logging_name = get_result_logging_name(result)
        logger.debug('-> Waiting for %s to become ready' % logging_name)
        try:
            while not is_result_ready(result):
                if RevokedRequests.instance().is_revoked(result):
                    raise ChainRevokedException(task_id=str(result),
                                                task_name=get_task_name_from_result(result))

                _check_for_traceback_in_parents(result)

                if max_trials and trials >= max_trials:
                    logging_name = get_result_logging_name(result)
                    raise WaitOnChainTimeoutError('Result ID %s was not ready in %d seconds' % (logging_name, max_wait))

                time.sleep(sleep_between_iterations)

                trials += 1

                # callbacks
                for callback in callbacks:
                    if trials % (callback.frequency / sleep_between_iterations) == 0:
                        callback.func(**callback.kwargs)

            if result.state == REVOKED:
                raise ChainRevokedException(task_id=str(result),
                                            task_name=get_task_name_from_result(result))
            if result.state == FAILURE:
                cause = result.result if isinstance(result.result, Exception) else None
                raise ChainInterruptedException(task_id=str(result),
                                                task_name=get_task_name_from_result(result),
                                                cause=cause)

        except (ChainRevokedException, ChainInterruptedException) as e:
            failures.append(e)

    if len(failures) == 1:
        raise failures[0]
    elif failures:
        multi_exception = MultipleFailuresException()
        multi_exception.failures = failures
        raise multi_exception


def wait_on_async_results_and_maybe_raise(results, raise_exception_on_failure=True, caller_task=None, **kwargs):
    try:
        wait_on_async_results(results=results, caller_task=caller_task, **kwargs)
    except ChainInterruptedException:
        if raise_exception_on_failure:
            raise


# This is a generator that returns one AsyncResult as it completes
def wait_for_any_results(results, max_wait=None, poll_max_wait=0.1, **kwargs):
    if isinstance(results, AsyncResult):
        results = [results]
    start_time = time.time()
    while len(results):
        if max_wait and max_wait < time.time() - start_time:
            raise WaitOnChainTimeoutError('Results %r were still not ready after %d seconds' % (results, max_wait))
        for result in results:
            try:
                wait_on_async_results([result], max_wait=poll_max_wait, **kwargs)
            except WaitOnChainTimeoutError:
                pass
            else:
                yield result
                results.remove(result)
                break


class WaitOnChainTimeoutError(Exception):
    pass


class ChainException(Exception):
    pass


class ChainRevokedException(ChainException):
    MESSAGE = "The chain has been interrupted by the revocation of microservice "

    def __init__(self, task_id=None, task_name=None):
        self.task_id = task_id
        self.task_name = task_name
        super(ChainRevokedException, self).__init__(task_id, task_name)

    def __str__(self):
        message = self.MESSAGE
        if self.task_name:
            message += self.task_name
        if self.task_id:
            message += '[%s]' % self.task_id
        return message


class ChainInterruptedException(ChainException):
    MESSAGE = "The chain has been interrupted by a failure in microservice "

    def __init__(self, task_id=None, task_name=None, cause=None):
        self.task_id = task_id
        self.task_name = task_name
        self.__cause__ = cause
        super(ChainInterruptedException, self).__init__(task_id, task_name, cause)

    def __str__(self):
        message = self.MESSAGE
        if self.task_name:
            message += self.task_name
        if self.task_id:
            message += '[%s]' % self.task_id
        return message


class MultipleFailuresException(ChainInterruptedException):
    MESSAGE = "The chain has been interrupted by multiple failing microservices"

    def __init__(self):
        super(ChainInterruptedException, self).__init__()

    def __str__(self):
        return self.MESSAGE


def get_task_results(results: dict) -> dict:
    try:
        return_keys = results[RETURN_KEYS_KEY]
    except KeyError:
        return {}
    else:
        return {k: results[k] for k in return_keys} if return_keys else {}


def _get_results(result: AsyncResult, return_keys_only=True, merge_children_results=False) -> dict:
    results = {}
    if not result:
        return results
    try:
        if result.successful():
            _results = result.result
            if _results:
                if return_keys_only:
                    results = get_task_results(_results)
                else:
                    # Delete the RETURN_KEYS_KEY
                    _results.pop(RETURN_KEYS_KEY, None)
                    results = _results

        if merge_children_results:
            children = result.children
            if children:
                for child in children:
                    child_results = get_results(child,
                                                return_keys_only=return_keys_only,
                                                merge_children_results=merge_children_results)
                    results.update(child_results)
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())

    return results


def get_results(result: AsyncResult,
                return_keys: Union[str, tuple] = (),
                return_keys_only: bool = True,
                merge_children_results: bool = False) -> Union[tuple, dict]:
    """
    Extract and return task results

    Args:
        result: The AsyncResult to extract actual returned results from
        return_keys: A single return key string, or a tuple of keys to extract from the AsyncResult.
            The default value of :const:`None` will return a dictionary of key/value pairs for the returned results.
        return_keys_only: If :const:`True` (default), only return results for keys specified by the task's
            `@returns` decorator or :attr:`returns` attribute. If :const:`False`, returns will include key/value pairs
            from the `bag of goodies`.
        merge_children_results: If :const:`True`, traverse children of `result`, and merge results produced by them.
            The default value of :const:`False` will not collect results from the children.

    Returns:
        If `return_keys` parameter was specified, returns a tuple of the results in the same order of the return_keys.
        If `return_keys` parameter wasn't specified, return a dictionary of the key/value pairs of the returned results.
    """
    extracted_dict = _get_results(result,
                                  return_keys_only=return_keys_only,
                                  merge_children_results=merge_children_results)
    return results2tuple(extracted_dict, return_keys) if return_keys else extracted_dict


def results2tuple(results: dict, return_keys: Union[str, tuple]) -> tuple:
    if isinstance(return_keys, str):
        return_keys = tuple([return_keys])
    return tuple([results.get(key) for key in return_keys])


def get_results_upto_parent(result: AsyncResult, parent_id: str = None, return_keys=(), **kwargs):
    extracted_dict = {}
    node = result
    while node and node.id != parent_id:
        node_results = get_results(node, **kwargs)
        for k, v in node_results.items():
            if k not in extracted_dict:
                # Since we're walking up the chain, children gets precedence in case we get the same key
                extracted_dict[k] = v
        node = node.parent
    return results2tuple(extracted_dict, return_keys) if return_keys else extracted_dict


def disable_async_result(result: AsyncResult):
    # fetching the children could itself result in using the backend. So we disable it before hand
    result.backend = None
    try:
        children = result.children or []
    except AttributeError:
        return

    for child in children:
        disable_async_result(child)
