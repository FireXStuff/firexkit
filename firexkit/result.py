import copy
import time
from collections import namedtuple
import weakref

import traceback
from pprint import pformat
from typing import Union, Iterator

from celery import current_app
from celery.result import AsyncResult
from celery.signals import before_task_publish, task_prerun, task_postrun
from celery.states import FAILURE, REVOKED, PENDING, STARTED, RECEIVED, RETRY, SUCCESS, READY_STATES
from celery.utils.log import get_task_logger
from firexkit.broker import handle_broker_timeout
from firexkit.inspect import get_task, get_active_queues, get_active, get_reserved
from firexkit.revoke import RevokedRequests

RETURN_KEYS_KEY = '__task_return_keys'
_TASK_POST_RUN_KEY = 'TASK_POST_RUN'

logger = get_task_logger(__name__)


def get_task_info_from_result(result, key: str = None):
    try:
        backend = result.app.backend
    except AttributeError:
        backend = current_app.backend

    if key is not None:
        info = handle_broker_timeout(backend.client.hget, args=(str(result), key))
    else:
        info = handle_broker_timeout(backend.client.get, args=(str(result), ))

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
    queue = ''
    try:
        while not queue and result:
            queue = get_task_info_from_result(result=result, key='queue')
            #  Try to get queue of parent tasks in the same chain, if current item hasn't been scheduled yet
            result = result.parent
    except AttributeError:
        logger.exception('Task queue info not supported for this broker')

    return queue


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
    callable_func = current_app.backend.client.hset
    args = (task_id, 'name', sender.name)
    handle_broker_timeout(callable_func=callable_func, args=args, timeout=5*60, reraise_on_timeout=False)


@task_postrun.connect
def mark_task_postrun(task, task_id, **_kwargs):
    task.backend.client.hset(task_id, _TASK_POST_RUN_KEY, 'True')


def get_task_postrun_info(result):
    postrun = True
    try:
        postrun = get_task_info_from_result(result, key=_TASK_POST_RUN_KEY)
    except AttributeError:
        logger.info(f'Broker doesn\'t support postrun info; probably a dummy broker. Defaulting to postrun=True')

    return postrun


def mark_queues_ready(*queue_names: str):
    current_app.backend.client.sadd('QUEUES', *queue_names)


def was_queue_ready(queue_name: str):
    return current_app.backend.client.sismember('QUEUES', queue_name)


def is_result_ready(result: AsyncResult, timeout=15*60, retry_delay=1):
    """
    Protect against broker being temporary unreachable and throwing a TimeoutError
    """
    return handle_broker_timeout(result.ready, timeout=timeout, retry_delay=retry_delay)


def find_all_unsuccessful(result: AsyncResult, ignore_non_ready=False, depth=0) -> {}:
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


def find_unsuccessful_in_chain(result: AsyncResult) -> {}:
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


def _check_for_failure_in_parents(result, timeout=15 * 60, retry_delay=1):
    failed_parent = revoked_parent = None
    parent = handle_broker_timeout(getattr, args=(result, 'parent'), timeout=timeout, retry_delay=retry_delay)
    while parent and parent != result:

        state = handle_broker_timeout(getattr, args=(parent, 'state'), timeout=timeout, retry_delay=retry_delay)
        if state == FAILURE:
            failed_parent = parent
            break

        if state == REVOKED or RevokedRequests.instance().is_revoked(parent):
            revoked_parent = parent
            break

        result = parent
        parent = handle_broker_timeout(getattr, args=(parent, 'parent'), timeout=timeout, retry_delay=retry_delay)
    else:
        return  # <-- loop finished with no errors in parents

    if revoked_parent:
        raise ChainRevokedException(task_id=str(revoked_parent),
                                    task_name=get_task_name_from_result(revoked_parent))

    #  If we get here, failed_parent holds a failed parent
    parent = handle_broker_timeout(getattr, args=(failed_parent, 'parent'), timeout=timeout, retry_delay=retry_delay)
    while parent and parent != failed_parent:  # Find first failed parent, now that celery propagates parent failures
        parent_failed = handle_broker_timeout(parent.failed,
                                              timeout=timeout,
                                              retry_delay=retry_delay)
        if not parent_failed:
            break

        failed_parent = parent
        parent = handle_broker_timeout(getattr, args=(parent, 'parent'), timeout=timeout,
                                       retry_delay=retry_delay)

    cause = handle_broker_timeout(getattr, args=(failed_parent, 'result'), timeout=timeout, retry_delay=retry_delay)
    cause = cause if isinstance(cause, Exception) else None
    raise ChainInterruptedException(task_id=str(failed_parent),
                                    task_name=get_task_name_from_result(failed_parent),
                                    cause=cause)


def _is_worker_alive(result: AsyncResult, retries=1):
    task_name = get_result_logging_name(result)
    tries = 0

    # NOTE: Retries for possible false negative in the case where task changes host in the small timing window
    # between getting task state / info and checking for aliveness. Retries for broker issues are handled downstream
    while tries <= retries:
        state = handle_broker_timeout(lambda r: r.state, args=(result,))
        if not state:
            logger.debug(f'Cannot get state for {task_name}; assuming task is alive')
            return True

        if state == STARTED or state == RECEIVED:
            # Query the worker to see if it knows about this task
            info = handle_broker_timeout(lambda r: r.info, args=(result,))
            try:
                # NOTE: if the task completes after the check for state right above but before the call
                # to handle_broker_timeout(), the type of 'info' is whatever the task returned, not the internal
                # Celery dictionary we want. It can be an exception, or even a dictionary with a random 'hostname'.
                # In the latter case _is_worker_alive() will return False, but since we retry _is_worker_alive() that
                # should be fine -- this timing issue cannot happen twice for the same task.
                hostname = info.get('hostname')
            except AttributeError:
                hostname = None

            if not hostname:
                logger.debug(f'Cannot get run info for {task_name}; assuming task is alive.'
                             f' Info: {info}, Hostname: {hostname}')
                return True

            task_id = result.id
            task_info = get_task(method_args=(task_id,), destination=(hostname,), timeout=60)
            if task_info and any(task_info.values()):
                return True

            # Try get_active and get_reserved, since we suspect query_task (the api used by get_task above)
            # may be broken sometimes.
            active_tasks = get_active(destination=(hostname,), timeout=60)
            task_list = active_tasks.get(hostname) if active_tasks else None
            if task_list:
                for task in task_list:
                    this_task_id = task.get('id')
                    if this_task_id == task_id:
                        return True

            reserved_tasks = get_reserved(destination=(hostname,), timeout=60)
            task_list = reserved_tasks.get(hostname) if reserved_tasks else None
            if task_list:
                for task in task_list:
                    this_task_id = task.get('id')
                    if this_task_id == task_id:
                        return True

            logger.debug(f'Task inspection for {task_name} on {hostname} with id '
                         f'of {task_id} returned:\n{pformat(task_info)}\n'
                         f'Active tasks:\n{pformat(active_tasks)}\n'
                         f'Reserved tasks:\n{pformat(reserved_tasks)}')

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

            queues = get_active_queues(timeout=60)
            active_queues = {queue['name'] for node in queues.values() for queue in node} if queues else set()
            if task_queue in active_queues:
                return True

            logger.debug(f'Active queues inspection for {task_name} on queue {task_queue} returned:\n'
                         f'{pformat(queues)}\n'
                         f'Active queues: {pformat(active_queues)}')

        elif state == SUCCESS:
            return True  # Timing; possible if task state changed after we waited on it but before we got here

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


def wait_for_running_tasks_from_results(results, max_wait=2*60, sleep_between_iterations=0.05):
    run_states = set(READY_STATES) | {STARTED, RETRY}
    running_tasks = []
    for result in results:
        if result.state in run_states and not get_task_postrun_info(result):
            running_tasks.append(result)

    max_sleep = sleep_between_iterations * 60  # Somewhat arbitrary
    start_time = last_debug_output = time.monotonic()
    while running_tasks:
        time_now = time.monotonic()

        if time_now - last_debug_output >= 30:
            logger.debug(f'Waiting for running task(s): {get_tasks_names_from_results(running_tasks)}')
            last_debug_output = time_now

        if max_wait and time_now - start_time >= max_wait:
            break

        time.sleep(sleep_between_iterations)
        running_tasks = [result for result in running_tasks if not get_task_postrun_info(result)]
        sleep_between_iterations = sleep_between_iterations * 1.01 \
            if sleep_between_iterations*1.01 < max_sleep else max_sleep

    if running_tasks:
        logger.error(f'The following tasks may still be running after task-wait timeout has expired:\n'
                     f'{get_tasks_names_from_results(running_tasks)}')


@send_block_task_states_to_caller_task
def wait_on_async_results(results,
                          max_wait=None,
                          callbacks: Iterator[WaitLoopCallBack] = tuple(),
                          sleep_between_iterations=0.05,
                          check_task_worker_frequency=600,
                          fail_on_worker_failures=7,
                          log_msg=True,
                          **_kwargs):
    if not results:
        return

    if isinstance(results, AsyncResult):
        results = [results]

    max_sleep = sleep_between_iterations * 20 * 15  # Somewhat arbitrary
    failures = []
    start_time = time.monotonic()
    last_callback_time = {callback.func: start_time for callback in callbacks}
    for result in results:
        logging_name = get_result_logging_name(result)
        if log_msg:
            logger.debug('-> Waiting for %s to complete' % logging_name)

        try:
            task_worker_failures = 0
            last_dead_task_worker_check = time.monotonic()
            while not is_result_ready(result):
                if RevokedRequests.instance().is_revoked(result):
                    break
                _check_for_failure_in_parents(result)

                current_time = time.monotonic()
                if max_wait and (current_time - start_time) > max_wait:
                    logging_name = get_result_logging_name(result)
                    raise WaitOnChainTimeoutError('Result ID %s was not ready in %d seconds' % (logging_name, max_wait))

                # callbacks
                for callback in callbacks:
                    if (current_time - last_callback_time[callback.func]) > callback.frequency:
                        callback.func(**callback.kwargs)
                        last_callback_time[callback.func] = current_time

                # Check for dead workers
                if check_task_worker_frequency and fail_on_worker_failures and \
                        (current_time - last_dead_task_worker_check) > check_task_worker_frequency:
                    alive = _is_worker_alive(result=result)
                    last_dead_task_worker_check = current_time
                    if not alive:
                        task_worker_failures += 1
                        logger.warning(f'Task {get_task_name_from_result(result)} appears to be a zombie.'
                                       f' Failures: {task_worker_failures}')
                        if task_worker_failures >= fail_on_worker_failures:
                            task_id = str(result)
                            task_name = get_task_name_from_result(result)
                            raise ChainInterruptedByZombieTaskException(task_id=task_id, task_name=task_name)
                    else:
                        task_worker_failures = 0

                time.sleep(sleep_between_iterations)
                sleep_between_iterations = sleep_between_iterations * 1.01 \
                    if sleep_between_iterations*1.01 < max_sleep else max_sleep  # Exponential backoff

            # If failure happened in a chain, raise from the failing task within the chain
            _check_for_failure_in_parents(result)

            result_state = handle_broker_timeout(getattr, args=(result, 'state'))
            if result_state == REVOKED:
                #  wait for revoked tasks to actually finish running
                wait_for_running_tasks_from_results([result])
                raise ChainRevokedException(task_id=str(result),
                                            task_name=get_task_name_from_result(result))
            if result_state == PENDING:
                # Pending tasks can be in revoke list. State will still be PENDING.
                raise ChainRevokedPreRunException(task_id=str(result),
                                                  task_name=get_task_name_from_result(result))
            if result_state == FAILURE:
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


def _warn_on_never_callback(callbacks, poll_max_wait):
    if callbacks:
        for will_not_run_callback in [c for c in callbacks if c.frequency > poll_max_wait]:
            logger.warning(f'Will not run {will_not_run_callback.func} due to frequency '
                           'being too high relative to any child poll rate.')


# This is a generator that returns one AsyncResult as it completes
def wait_for_any_results(results, max_wait=None, poll_max_wait=0.1, log_msg=False,
                         callbacks: Iterator[WaitLoopCallBack] = tuple(),
                         **kwargs):
    if isinstance(results, AsyncResult):
        results = [results]

    _warn_on_never_callback(callbacks, poll_max_wait)

    start_time = time.time()

    logging_names = [f'-> {get_result_logging_name(result)}' for result in results]

    logger.debug('Waiting for any of the following tasks to complete:\n' + '\n'.join(logging_names))
    while len(results):
        if max_wait and max_wait < time.time() - start_time:
            raise WaitOnChainTimeoutError('Results %r were still not ready after %d seconds' % (results, max_wait))
        for result in results:
            try:
                wait_on_async_results_and_maybe_raise([result], max_wait=poll_max_wait, log_msg=log_msg, callbacks=callbacks, **kwargs)
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


def get_tasks_inputs_from_result(results: dict) -> dict:
    # Returns a dict of key-value pairs of inputs passed down in the async result object
    try:
        return_keys = list(results[RETURN_KEYS_KEY])
    except KeyError:
        return results
    else:
        return_keys.append(RETURN_KEYS_KEY)
        return {k: v for k, v in results.items() if k not in return_keys}


def _get_results(result: AsyncResult, return_keys_only=True, merge_children_results=False) -> dict:
    # Generally,
    #   1.	Apply inputs from current result
    #   2.	Apply child results (and inputs) from children (if merge_children_results is True)
    #   3.	Apply output from current result

    results = {}
    returned_values = {}
    if not result:
        return results
    try:
        if result.successful():
            _results = copy.deepcopy(result.result) if isinstance(result.result, dict) else result.result
            if _results:
                # these will need to be updated only after the children results are extracted
                # i.e., node results are applied after children results
                returned_values = get_task_results(_results)
                if not return_keys_only:
                    # Only relevant if merge_children_results is True:
                    #   If return_keys_only us False, we need to apply the inputs first
                    #   before applying the results(and inputs) of the children
                    results = get_tasks_inputs_from_result(_results)

        if merge_children_results:
            children = result.children
            if children:
                for child in children:
                    child_results = get_results(child,
                                                return_keys_only=return_keys_only,
                                                merge_children_results=merge_children_results)
                    results.update(child_results)

        results.update(returned_values)
    except Exception as e:
        logger.exception(e)

    return results


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


def get_results(result: AsyncResult,
                return_keys=(),
                parent_id: str = None,
                return_keys_only=True,
                merge_children_results=False,
                extract_from_parents=True):
    """
    Extract and return task results

    Args:
        result: The AsyncResult to extract actual returned results from
        return_keys: A single return key string, or a tuple of keys to extract from the AsyncResult.
            The default value of :const:`None` will return a dictionary of key/value pairs for the returned results.
        return_keys_only: If :const:`True` (default), only return results for keys specified by the task's
            `@returns` decorator or :attr:`returns` attribute. If :const:`False`, returns will include key/value pairs
            from the `bag of goodies`.
        parent_id: If :attr:`extract_from_parents` is set, extract results up to this parent_id, or until we can no
            longer traverse up the parent hierarchy
        merge_children_results: If :const:`True`, traverse children of `result`, and merge results produced by them.
            The default value of :const:`False` will not collect results from the children.
        extract_from_parents: If :const:`True` (default), will consider all results returned from tasks of the given
            chain (parents of the last task). Else will consider only results returned by the last task of the chain.

    Returns:
        If `return_keys` parameter was specified, returns a tuple of the results in the same order of the return_keys.
        If `return_keys` parameter wasn't specified, return a dictionary of the key/value pairs of the returned results.
    """
    def _update_results_dict(n):
        node_results = _get_results(n,
                                    return_keys_only=return_keys_only,
                                    merge_children_results=merge_children_results)
        for k, v in node_results.items():
            if k not in extracted_dict:
                # Since we're walking up the chain, children gets precedence in case we get the same key
                extracted_dict[k] = v

    extracted_dict = {}

    # Get results from current AsyncResult object
    node = result
    _update_results_dict(node)

    # Get results from parents, if applicable
    if node:
        stop_at = node.id if extract_from_parents is False else parent_id
        while node.id != stop_at:
            node = node.parent
            if node:
                _update_results_dict(node)
            else:
                break

    from firexkit.task import FireXTask
    if not return_keys or return_keys == FireXTask.DYNAMIC_RETURN or return_keys == (FireXTask.DYNAMIC_RETURN,):
        return extracted_dict
    else:
        return results2tuple(extracted_dict, return_keys)


FIREX_AR_REFS_ATTR = '__firex_ar_refs__'


def is_async_result_monkey_patched_to_track():
    return hasattr(AsyncResult, FIREX_AR_REFS_ATTR)


def teardown_monkey_patch_async_result_to_track_instances():

    if hasattr(AsyncResult, '__orig_init__'):
        AsyncResult.__init__ = AsyncResult.__orig_init__
        delattr(AsyncResult, '__orig_init__')
    try:
        delattr(AsyncResult, FIREX_AR_REFS_ATTR)
    except AttributeError:
        pass


def monkey_patch_async_result_to_track_instances():
    assert not is_async_result_monkey_patched_to_track(), "Cannot monkey patch to track AsyncResults twice."

    AsyncResult.__orig_init__ = AsyncResult.__init__
    setattr(AsyncResult, FIREX_AR_REFS_ATTR, [])

    def tracking_init(self, *args, **kwargs):
        getattr(AsyncResult, FIREX_AR_REFS_ATTR).append(weakref.ref(self))
        AsyncResult.__orig_init__(self, *args, **kwargs)

    def get_ar_instances() -> Iterator[AsyncResult]:
        for inst_ref in getattr(AsyncResult, FIREX_AR_REFS_ATTR):
            inst = inst_ref()
            if inst is not None:
                yield inst

    AsyncResult.__init__ = tracking_init
    AsyncResult.get_ar_instances = get_ar_instances


def disable_all_async_results():
    if is_async_result_monkey_patched_to_track():
        for async_result in AsyncResult.get_ar_instances():
            async_result.backend = None


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


#
# Returns the last exception in the cause chain that is a "ChainInterruptedException"
#
def last_causing_chain_interrupted_exception(ex):
    e = ex
    while e.__cause__ is not None and isinstance(e.__cause__, ChainInterruptedException):
        e = e.__cause__
    return e
