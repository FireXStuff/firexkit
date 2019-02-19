import time
from collections import namedtuple

from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from firexkit.revoke import RevokedRequests

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


def get_result_logging_name(result, name=None):
    if name is None:
        name = get_task_name_from_result(result)
    return '%s[%s]' % (name, result)


def is_result_ready(result, max_trials=None, retry_delay=1):
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
