import copy
import time
from collections import namedtuple

import traceback
from pprint import pformat
from typing import Union

from celery import current_app
from celery.result import AsyncResult
from celery.signals import before_task_publish, task_prerun
from celery.states import FAILURE, REVOKED, PENDING, STARTED, RECEIVED, RETRY
from celery.utils.log import get_task_logger
from firexkit.broker import handle_broker_timeout
from firexkit.inspect import get_task, get_active_queues
from firexkit.revoke import RevokedRequests

RETURN_KEYS_KEY = '__task_return_keys'

logger = get_task_logger(__name__)


def get_task_info_from_result(result, key: str = None):
    try:
        backend = result.app.backend
    except AttributeError:
        backend = current_app.backend

    if key is not None:
        info = handle_broker_timeout(backend.client.hget, args=(str(result), key), timeout=30)
    else:
        info = handle_broker_timeout(backend.client.get, args=(str(result), ), timeout=30)

    if info is None:
        info = ''
    else:
        info = info.decode()
    return info


def get_task_name_from_result(result):
    try:
        return get_task_info_from_result(result=result, key='name')
    except AttributeError:
        return get_task_info_from_result(result=result)


def get_task_queue_from_result(result):
    try:
        return get_task_info_from_result(result=result, key='queue')
    except AttributeError:
        logger.exception('Task queue info not supported for this broker')
        return ''


def get_tasks_names_from_results(results):
    return [get_result_logging_name(r) for r in results]


def get_result_logging_name(result: AsyncResult, name=None):
    if name is None:
        name = get_task_name_from_result(result)
    return '%s[%s]' % (name, result)


@before_task_publish.connect
def populate_task_info(sender, declare, headers, **_kwargs):
    task_info = {'name': sender}
    try:
        task_info.update({'queue': declare[0].name})
    except (IndexError, AttributeError):
        pass

    current_app.backend.client.hmset(headers['id'], task_info)


@task_prerun.connect
def update_task_name(sender, task_id, *_args, **_kwargs):
    # Although the name was populated in populate_task_info before_task_publish, the name
    # can be inaccurate if it was a plugin. We can only over-write it with the accurate name
    # at task_prerun.
    task_info = {'name': sender.name}
    current_app.backend.client.hmset(task_id, task_info)


def mark_queues_ready(*queue_names: str):
    current_app.backend.client.sadd('QUEUES', *queue_names)


def was_queue_ready(queue_name: str):
    return current_app.backend.client.sismember('QUEUES', queue_name)


def is_result_ready(result: AsyncResult, timeout=None, retry_delay=0.1):
    """
    Protect against broker being temporary unreachable and throwing a TimeoutError
    """
    return handle_broker_timeout(result.ready, timeout=timeout, retry_delay=retry_delay)


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


def _check_for_traceback_in_parents(result, timeout=None, retry_delay=0.1):
    parent = handle_broker_timeout(getattr, args=(result, 'parent'), timeout=timeout, retry_delay=retry_delay)
    if parent:
        parent_failed = handle_broker_timeout(parent.failed, timeout=timeout, retry_delay=retry_delay)
        if parent_failed:
            cause = handle_broker_timeout(getattr, args=(parent, 'result'), timeout=timeout, retry_delay=retry_delay)
            cause = cause if isinstance(cause, Exception) else None
            raise ChainInterruptedException(task_id=str(parent),
                                            task_name=get_task_name_from_result(parent),
                                            cause=cause)
        elif handle_broker_timeout(getattr, args=(parent, 'state'), timeout=timeout, retry_delay=retry_delay) == REVOKED:
            raise ChainRevokedException(task_id=str(parent),
                                        task_name=get_task_name_from_result(parent))
        else:
            return _check_for_traceback_in_parents(parent, timeout=timeout, retry_delay=retry_delay)


def _is_worker_alive(result: AsyncResult, timeout=None, retry_delay=0.1, retries=1):
    task_name = get_result_logging_name(result)
    tries = 0

    # NOTE: Retries for possible false negative in the case where task changes host in the small timing window
    # between getting task state / info and checking for aliveness. Retries for broker issues are handled downstream
    while tries <= retries:
        state = handle_broker_timeout(lambda r: r.state, args=(result,), timeout=timeout, retry_delay=retry_delay)
        if not state:
            logger.debug(f'Cannot get state for {task_name}; assuming task is alive')
            return True

        if state == STARTED or state == RECEIVED:
            # Query the worker to see if it knows about this task
            info = handle_broker_timeout(lambda r: r.info, args=(result,), timeout=timeout, retry_delay=retry_delay)
            hostname = info.get('hostname') if info else None

            if not hostname:
                logger.debug(f'Cannot get run info for {task_name}; assuming task is alive.'
                             f' Info: {info}, Hostname: {hostname}')
                return True

            task_info = get_task(method_args=(result.id,), destination=(hostname,), timeout=10)
            if task_info and any(task_info.values()):
                return True

            logger.debug(f'Task inspection for {task_name} on {hostname} returned:\n{pformat(task_info)}')

        elif state == PENDING or state == RETRY:
            # Check if task queue is alive
            task_queue = get_task_queue_from_result(result)
            if not task_queue:
                logger.debug(f'Cannot get task queue for {task_name}; assuming task is alive.')
                return True

            queue_seen = was_queue_ready(queue_name=task_queue)
            if not queue_seen:
                logger.debug(f'Queue "{task_queue}" for {task_name} not seen yet; assuming task is alive.')
                return True

            queues = get_active_queues()
            active_queues = {queue['name'] for node in queues.values() for queue in node} if queues else set()
            if task_queue in active_queues:
                return True

            logger.debug(f'Active queues inspection for {task_name} on queue {task_queue} returned:\n'
                         f'{pformat(queues)}\n'
                         f'Active queues: {pformat(active_queues)}')

        else:
            logger.debug(f'Unknown state ({state} for task {task_name}; assuming task is alive.')
            return True

        tries += 1
        logger.info(f'Task {task_name} is not responding to queries. Tries: {tries}')

    return False


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
                          sleep_between_iterations=0.05,
                          check_task_worker_frequency=900,
                          fail_on_worker_failures=3,
                          log_msg=True
                          ):
    if not results:
        return

    max_trials = max_wait/sleep_between_iterations if max_wait else None
    trials = 0

    if isinstance(results, AsyncResult):
        results = [results]

    failures = []
    for result in results:
        logging_name = get_result_logging_name(result)
        if log_msg:
            logger.debug('-> Waiting for %s to complete' % logging_name)
        try:
            worker_failures = 0
            while not is_result_ready(result):
                if RevokedRequests.instance().is_revoked(result):
                    break

                _check_for_traceback_in_parents(result, timeout=30)

                if max_trials and trials >= max_trials:
                    logging_name = get_result_logging_name(result)
                    raise WaitOnChainTimeoutError('Result ID %s was not ready in %d seconds' % (logging_name, max_wait))

                time.sleep(sleep_between_iterations)

                trials += 1

                # callbacks
                for callback in callbacks:
                    if trials % (callback.frequency / sleep_between_iterations) == 0:
                        callback.func(**callback.kwargs)

                # Check for dead workers
                if check_task_worker_frequency and fail_on_worker_failures and \
                        round(trials % (check_task_worker_frequency / sleep_between_iterations)) == 0:
                    alive = _is_worker_alive(result=result)
                    if not alive:
                        worker_failures += 1
                        logger.warning(f'Task {get_task_name_from_result(result)} appears to be a zombie.'
                                       f' Failures: {worker_failures}')
                        if worker_failures >= fail_on_worker_failures:
                            raise ChainInterruptedByZombieTaskException(task_id=str(result),
                                                                        task_name=get_task_name_from_result(result))
                    else:
                        worker_failures = 0

            if result.state == REVOKED:
                raise ChainRevokedException(task_id=str(result),
                                            task_name=get_task_name_from_result(result))
            if result.state == PENDING:
                # Pending tasks can be in revoke list. State will still be PENDING.
                raise ChainRevokedPreRunException(task_id=str(result),
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
        failed_task_ids = [e.task_id for e in failures if hasattr(e, 'task_id')]
        multi_exception = MultipleFailuresException(failed_task_ids)
        multi_exception.failures = failures
        raise multi_exception


def wait_on_async_results_and_maybe_raise(results, raise_exception_on_failure=True, caller_task=None, **kwargs):
    try:
        wait_on_async_results(results=results, caller_task=caller_task, **kwargs)
    except ChainInterruptedException:
        if raise_exception_on_failure:
            raise


# This is a generator that returns one AsyncResult as it completes
def wait_for_any_results(results, max_wait=None, poll_max_wait=0.1, log_msg=False, **kwargs):
    if isinstance(results, AsyncResult):
        results = [results]
    start_time = time.time()

    logging_names = [f'-> {get_result_logging_name(result)}' for result in results]

    logger.debug('Waiting for any of the following tasks to complete:\n' + '\n'.join(logging_names))
    while len(results):
        if max_wait and max_wait < time.time() - start_time:
            raise WaitOnChainTimeoutError('Results %r were still not ready after %d seconds' % (results, max_wait))
        for result in results:
            try:
                wait_on_async_results_and_maybe_raise([result], max_wait=poll_max_wait, log_msg=log_msg, **kwargs)
            except WaitOnChainTimeoutError:
                pass
            else:
                yield result
                logger.debug(f'--> {get_result_logging_name(result)} completed')
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


class ChainRevokedPreRunException(ChainRevokedException):
    pass


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


class ChainInterruptedByZombieTaskException(ChainInterruptedException):
    def __str__(self):
        return super().__str__() + ': (zombie task)'


class MultipleFailuresException(ChainInterruptedException):
    MESSAGE = "The chain has been interrupted by multiple failing microservices: %s"

    def __init__(self, task_ids=('UNKNOWN',)):
        self.task_ids = task_ids
        super(ChainInterruptedException, self).__init__()

    def __str__(self):
        return self.MESSAGE % self.task_ids


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
            _results = copy.deepcopy(result.result) if isinstance(result.result, dict) else result.result
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
    from firexkit.task import FireXTask
    if not return_keys or return_keys == FireXTask.DYNAMIC_RETURN or return_keys == (FireXTask.DYNAMIC_RETURN, ):
        return extracted_dict
    else:
        return results2tuple(extracted_dict, return_keys)


def results2tuple(results: dict, return_keys: Union[str, tuple]) -> tuple:
    from firexkit.task import FireXTask
    if isinstance(return_keys, str):
        return_keys = tuple([return_keys])
    results_to_return = []
    for key in return_keys:
        if key == FireXTask.DYNAMIC_RETURN:
            results_to_return.append(results)
        else:
            results_to_return.append(results.get(key))
    return tuple(results_to_return)


def get_results_upto_parent(result: AsyncResult, return_keys=(), parent_id: str = None, **kwargs):
    extracted_dict = {}
    node = result
    while node and node.id != parent_id:
        node_results = get_results(node, **kwargs)
        for k, v in node_results.items():
            if k not in extracted_dict:
                # Since we're walking up the chain, children gets precedence in case we get the same key
                extracted_dict[k] = v
        node = node.parent

    from firexkit.task import FireXTask
    if not return_keys or return_keys == FireXTask.DYNAMIC_RETURN or return_keys == (FireXTask.DYNAMIC_RETURN,):
        return extracted_dict
    else:
        return results2tuple(extracted_dict, return_keys)


def disable_async_result(result: AsyncResult):
    # fetching the children could itself result in using the backend. So we disable it before hand
    result.backend = None
    try:
        children = result.children or []
    except AttributeError:
        return

    for child in children:
        disable_async_result(child)

#
# Returns the first exception that is not a "ChainInterruptedException"
# in the exceptions stack.
#
def first_non_chain_interrupted_exception(ex):
    e = ex
    while e.__cause__ is not None and isinstance(e, ChainInterruptedException):
        e = e.__cause__
    return e


def extract_and_filter(*args,
                       extract_from_children: bool = True,
                       extract_task_returns_only: bool = False,
                       extract_from_parents: bool = False,
                       **kwargs) -> Union[tuple, dict]:
    """Wrapper for :func:`firexkit.result.get_results` and `firexkit.result.get_results_upto_parent`
    that defaults `extract_from_children` to :const:`True` and `extract_task_returns_only` to :const:`False`

    This wrapper only exists for legacy reasons since older chains might be relying on these old defaults for
    `extract_from_children` and `extract_task_returns_only`.

    Use :meth:`firexkit.result.get_results` or `firexkit.result.get_results_upto_parent` instead.

    See Also:
        firexkit.result.get_results
    """

    if extract_from_parents:
        return get_results_upto_parent(*args,
                                       return_keys_only=extract_task_returns_only,
                                       merge_children_results=extract_from_children,
                                       **kwargs)
    else:
        return get_results(*args,
                           return_keys_only=extract_task_returns_only,
                           merge_children_results=extract_from_children,
                           **kwargs)