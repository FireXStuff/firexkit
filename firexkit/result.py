import time
from collections import namedtuple

import traceback
from celery.result import AsyncResult
from celery.signals import task_prerun
from celery.states import FAILURE
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


def is_result_ready(result: AsyncResult, max_trials=None, retry_delay=1):
    """
    Protect against broker being temporary unreachable and throwing a TimeoutError
    """
    trials = 0
    while True:
        try:
            return result.ready()
        except Exception as e:
            # need to handle different timeout exceptions from different brokers
            if type(e).__name__ != "TimeoutError":
                raise

            trials += 1
            logger.warning('Backend was not reachable and timed out; trial %d' % trials)
            if max_trials and trials >= max_trials:
                logger.error('Reached max_trials of %d...giving up!' % max_trials)
                raise
            else:
                time.sleep(retry_delay)


def find_unsuccessful(result: AsyncResult, ignore_non_ready=False, depth=0)->{}:
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
            failures.update(find_unsuccessful(child, ignore_non_ready, depth))
    return failures


def _check_for_traceback_in_parents(result):
    parent = result.parent
    if parent:
        if parent.failed():
            raise ChainInterruptedException(get_result_logging_name(parent))
        elif parent.state == 'REVOKED':
            raise ChainRevokedException(get_result_logging_name(parent))
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
def wait_on_async_results(results, max_wait=None, depth=1, callbacks: [WaitLoopCallBack]=tuple(),
                          sleep_between_iterations=0.1):
    if not results:
        return

    max_trials = max_wait/sleep_between_iterations if max_wait else None
    trials = 0

    if isinstance(results, AsyncResult):
        results = [results]

    for result in results:
        name = get_result_logging_name(result)
        logger.debug('-'*depth*2 + '> Waiting for %s to become ready' % name)
        while not is_result_ready(result) and not RevokedRequests.instance().is_revoked(result):

            _check_for_traceback_in_parents(result)

            if max_trials and trials >= max_trials:
                raise WaitOnChainTimeoutError('Result ID %s was not ready in %d seconds' % (name, max_wait))
            time.sleep(sleep_between_iterations)
            trials += 1

            # callbacks
            for callback in callbacks:
                if trials % (callback.frequency / sleep_between_iterations) == 0:
                    callback.func(**callback.kwargs)
        if result.state == 'REVOKED' and depth == 1:
            raise ChainRevokedException(name)
        children = result.children
        if children:
            for child in children:
                if child == result:
                    logger.error('OOPS: INFINITE RECURSION WAS BLOCKED for %s' % name)
                else:
                    wait_on_async_results(child, max_wait, depth + 1, callbacks,
                                          sleep_between_iterations=sleep_between_iterations)


def wait_on_async_result_and_maybe_raise(result, raise_exception_on_failure=True, caller_task=None):
    try:
        wait_on_async_results(results=result, caller_task=caller_task)
        logger.debug(get_result_logging_name(result) + ' completed. Unblocking.')
        if result.failed():
            raise ChainInterruptedException(get_result_logging_name(result))
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


class ChainRevokedException(Exception):
    MESSAGE = "The chain has been interrupted by the revocation of microservice %s"

    def __init__(self, microservice_name):
        super(ChainRevokedException, self).__init__(self.MESSAGE % microservice_name)


class ChainInterruptedException(Exception):
    MESSAGE = "The chain has been interrupted by a failure in microservice %s"

    def __init__(self, microservice_name):
        super(ChainInterruptedException, self).__init__(self.MESSAGE % microservice_name)


def get_task_results(results: dict) -> dict:
    try:
        return_keys = results[RETURN_KEYS_KEY]
    except KeyError:
        return {}
    else:
        return {k: results[k] for k in return_keys} if return_keys else {}


def _get_results(result: AsyncResult, return_keys_only=True, merge_children_results=False) -> dict:
    results = {}
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


def get_results(result: AsyncResult, return_keys=(), return_keys_only=True, merge_children_results=False):
    extracted_dict = _get_results(result,
                                  return_keys_only=return_keys_only,
                                  merge_children_results=merge_children_results)
    if return_keys:
        if isinstance(return_keys, str):
            return_keys = tuple([return_keys])
        return tuple([extracted_dict.get(key) for key in return_keys])
    else:
        return extracted_dict


def disable_async_result(result: AsyncResult):
    # fetching the children could itself result in using the backend. So we disable it before hand
    result.backend = None
    try:
        children = result.children or []
    except AttributeError:
        return

    for child in children:
        disable_async_result(child)
