import copy
import time
from collections import namedtuple, deque
import weakref

from pprint import pformat
from typing import Union, Iterator, Optional, Iterable, Any

from celery import current_app
from celery.result import AsyncResult
from celery.signals import before_task_publish, task_prerun, task_postrun
from celery.states import FAILURE, REVOKED, PENDING, STARTED, RECEIVED, RETRY, SUCCESS, READY_STATES
from celery.utils.log import get_task_logger
from firexkit.broker import handle_broker_timeout
from firexkit.revoke import RevokedRequests
from firexkit.firex_worker import FireXWorkerId, FireXCeleryInspector

RETURN_KEYS_KEY = '__task_return_keys'
_TASK_POST_RUN_KEY = 'TASK_POST_RUN'


RUN_RESULTS_NAME = 'chain_results'
RUN_UNSUCCESSFUL_NAME = 'unsuccessful_services'

logger = get_task_logger(__name__)

class _Unset:
    pass

_UNSET = _Unset()

class FireXAsyncResult(AsyncResult):

    # AsyncResult objects cannot be in memory after the broker (i.e. backend) shutdowns, otherwise errors are
    # produced when they are garbage collected. We therefore monkey patch AsyncResults to track all instances
    # (e.g. from unpickle, instantiated directly, etc) so that disable_all_async_results can disable their
    # references to the backend.
    _ar_refs : list[weakref.ReferenceType['FireXAsyncResult']] = []

    def __init__(self, *args, **kwargs):
        FireXAsyncResult._ar_refs.append(weakref.ref(self))
        super().__init__(*args, **kwargs)

        self._worker_id : Union[FireXWorkerId, _Unset, None] = _UNSET
        self._log_name : Optional[str] = None

    @ property
    def log_name(self) -> str:
        if self._log_name is None:
            self._log_name = get_task_name_from_result(self)
        return self._log_name

    def get_worker_id(self) -> Optional[FireXWorkerId]:
        if isinstance(self._worker_id, _Unset):
            celery_hostname = self._get_celery_hostname()
            if celery_hostname and '@' in celery_hostname:
                self._worker_id = FireXWorkerId.worker_id_from_str(celery_hostname)
            else:
                self._worker_id = None
        return self._worker_id

    def _get_celery_hostname(self) -> Optional[str]:
        info = handle_broker_timeout(lambda r: r.info, args=(self,))
        try:
            # NOTE: if the task completes after the check for state right above but before the call
            # to handle_broker_timeout(), the type of 'info' is whatever the task returned, not the internal
            # Celery dictionary we want. It can be an exception, or even a dictionary with a random 'hostname'.
            # In the latter case _is_worker_alive() will return False, but since we retry _is_worker_alive() that
            # should be fine -- this timing issue cannot happen twice for the same task.
            hostname = info.get('hostname')
        except (AttributeError, TypeError):
            logger.error(f'Cannot get hostname from {info}')
        else:
            if isinstance(hostname, str):
                return hostname
            else:
                logger.error(f'Ignoring unexpected task {self.id} hostname {hostname}')
        return None

    def fx_ready(self, timeout=15*60, retry_delay=1) -> bool:
        ready = bool(
            handle_broker_timeout(self.ready, timeout=timeout, retry_delay=retry_delay)
        )
        if ready:
            return True
        return RevokedRequests.instance().is_revoked(self.id)

    def fx_wait(self, max_wait=None):
        wait_on_async_results(self, max_wait=max_wait)

    def fx_state(self, timeout=15 * 60, retry_delay=1) -> Optional[str]:
        celery_state = handle_broker_timeout(
            getattr, args=(self, 'state'),
            timeout=timeout,
            retry_delay=retry_delay)
        if celery_state in [SUCCESS, FAILURE, REVOKED]:
            return celery_state
        if RevokedRequests.instance().is_revoked(self.id):
            return REVOKED
        return celery_state

    def fx_failed(self) -> bool:
        return bool(
            handle_broker_timeout(self.failed))

    def fx_parent(self) -> Optional['FireXAsyncResult']:
        return handle_broker_timeout(
            getattr,
            args=(self, 'parent'),
        )

    def fx_result(self) -> Any:
        handle_broker_timeout(
            getattr,
            args=(self, 'result'),
        )

    def fx_postrun_complete(self) -> bool:
        return get_task_postrun_info(self)

    def chain_root_ar(self) -> 'FireXAsyncResult':
        r = self
        parent = r.fx_parent()
        while parent is not None and r != parent:
            r = parent
            parent = parent.fx_parent()
        return r

    def disable_backend(self):
        self.backend = None

    @classmethod
    def _get_async_results(cls) -> list['FireXAsyncResult']:
        return [
            ar for ar in [
                ar_ref() for ar_ref in cls._ar_refs
            ] if ar
        ]

    @classmethod
    def disable_all_async_results(cls, extra_ar: 'FireXAsyncResult'):
        for ar in cls._get_async_results():
            ar.disable_backend()
        extra_ar.disable_backend()
        try:
            children = extra_ar.children or []
        except AttributeError:
            pass
        else:
            for child in children:
                child.disable_backend()





def _get_task_info_from_result(result: FireXAsyncResult, key: Optional[str]=None) -> str:
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
        return _get_task_info_from_result(result=result, key='name')
    except AttributeError:
        return _get_task_info_from_result(result=result)


def _get_task_queue_from_result(result: FireXAsyncResult):
    queue = ''
    try:
        while not queue and result:
            queue = _get_task_info_from_result(result=result, key='queue')
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
    handle_broker_timeout(
        callable_func=current_app.backend.client.hset,
        args=(task_id, 'name', sender.name),
        timeout=5*60,
        reraise_on_timeout=False)


@task_postrun.connect
def mark_task_postrun(task, task_id, **_kwargs):
    task.backend.client.hset(task_id, _TASK_POST_RUN_KEY, 'True')


def get_task_postrun_info(result: FireXAsyncResult) -> bool:
    postrun = True
    try:
        postrun = bool(_get_task_info_from_result(result, key=_TASK_POST_RUN_KEY))
    except AttributeError:
        logger.info('Broker doesn\'t support postrun info; probably a dummy broker. Defaulting to postrun=True')

    return postrun


def mark_queues_ready(*queue_names: str):
    current_app.backend.client.sadd('QUEUES', *queue_names)


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


def create_unsuccessful_result(failures: list[str], did_not_run: list[str]) -> dict[str, list[str]]:
    res = {}
    if failures:
        res['failed'] = failures
    if did_not_run:
        res['not_run'] = did_not_run
    return res


def find_unsuccessful_in_chain(result: AsyncResult) -> dict[str, list[str]]:
    failures = []
    did_not_run = []
    async_result : Optional[AsyncResult] = result
    while async_result:
        if is_result_ready(async_result):
            # Did this task fail?
            if async_result.state == FAILURE:
                failures.append(async_result)
        else:
            # This task was not ready
            did_not_run.append(async_result)
        async_result = async_result.parent
    # Should reverse the items since we're traversing the chain from RTL
    failures.reverse()
    did_not_run.reverse()
    return create_unsuccessful_result(failures, did_not_run)


def _check_for_failure_in_parents(result: FireXAsyncResult):
    failed_parent : Optional[FireXAsyncResult] = None
    revoked_parent : Optional[FireXAsyncResult] = None
    parent = result.fx_parent()
    while parent and parent != result:
        state = parent.fx_state()
        if state == FAILURE:
            failed_parent = parent
            break

        if state == REVOKED:
            revoked_parent = parent
            break

        result = parent
        parent = parent.fx_parent()

    if revoked_parent:
        raise ChainRevokedException.from_ar(revoked_parent)

    if failed_parent:
        #  If we get here, failed_parent holds a failed parent
        parent = failed_parent.fx_parent()
        while parent and parent != failed_parent:  # Find first failed parent, now that celery propagates parent failures
            if not parent.fx_failed():
                break

            failed_parent = parent
            parent = parent.fx_parent()

        failed_exc = failed_parent.fx_result()
        cause = failed_exc if isinstance(failed_exc, Exception) else None
        raise ChainInterruptedException.from_ar(failed_parent, cause=cause)


def _is_worker_alive(
    result: FireXAsyncResult,
    check_task_worker_timeout: int,
    check_task_worker_inspect_retry_timeout: int,
) -> bool:
    task_name = result.log_name
    retries = 1
    tries = 0

    logger.error(f'CHECKING {result.id} WORKER ALIVE {check_task_worker_timeout} and retry {check_task_worker_inspect_retry_timeout}')

    inspect = FireXCeleryInspector(
        current_app,
        inspect_timeout=check_task_worker_timeout,
        retry_timeout=check_task_worker_inspect_retry_timeout,
        # retry_on_none_resp=False,
    )

    # NOTE: Retries for possible false negative in the case where task changes host in the small timing window
    # between getting task state / info and checking for aliveness. Retries for broker issues are handled downstream
    while tries <= retries:
        logger.error('check state')
        state = result.fx_state(timeout=check_task_worker_timeout)
        logger.error('done state')
        if not state:
            logger.error(f'Cannot get state for {task_name}; assuming task is alive')
            return True

        if state in [RECEIVED, STARTED]:
            # Query the worker to see if it knows about this task
            worker_id = result.get_worker_id()
            if not worker_id:
                logger.error(f'Cannot get task info for {task_name}; assuming task is alive.')
                return True

            task_id = result.id
            logger.error('check get_task')
            task_info = task_id and inspect.get_task(task_id, worker_ids=[worker_id])
            logger.error('done get_task')
            if task_info:
                # can't tell if remote proc is alive, not worth ssh, just assume alive.
                if not worker_id.on_cur_host():
                    return True
                worker_and_parent_alive = task_info.worker_and_parent_procs_alive(on_error=None)
                if worker_and_parent_alive is not None:
                    return worker_and_parent_alive

            # Try get_active and get_reserved, since we suspect query_task (the api used by get_task above)
            # may be broken sometimes.
            active_tasks = inspect.get_single_worker_active_tasks(worker_id)
            for active_task in active_tasks:
                if active_task.id == task_id:
                    logger.error('Alive due to worker having tasks.')
                    return True

            reserved_tasks = inspect.get_single_worker_reserved_tasks(worker_id)
            for reserved_task in reserved_tasks:
                if reserved_task.id == task_id:
                    logger.error(f'Alive due to worker having reserved.')
                    return True

            logger.error(
                f'Task inspection for {task_name} on {worker_id} with id '
                f'of {task_id} returned:\n{pformat(task_info)}\n'
                f'Active tasks:\n{pformat(active_tasks)}\n'
                f'Reserved tasks:\n{pformat(reserved_tasks)}')

        elif state == PENDING or state == RETRY:
            # Check if task queue is alive
            task_queue = _get_task_queue_from_result(result)
            if not task_queue:
                logger.error(f'Cannot get task queue for {task_name}; assuming task is alive.')
                return True

            queue_seen = current_app.backend.client.sismember('QUEUES', task_queue)
            if not queue_seen:
                logger.error(f'Queue "{task_queue}" for {task_name} not seen yet; assuming task is alive.')
                return True

            queues_by_worker = inspect.get_active_tasks_by_worker_id()
            active_queues_names = {
                queue.name
                for worker_queues in queues_by_worker.values()
                for queue in worker_queues}

            if task_queue in active_queues_names:
                return True

            logger.error(f'Active queues inspection for {task_name} on queue {task_queue} returned:\n'
                         f'{pformat(queues_by_worker)}\n'
                         f'Active queues: {pformat(active_queues_names)}')

        elif state == SUCCESS:
            return True  # Timing; possible if task state changed after we waited on it but before we got here
        else:
            logger.error(f'Unknown state ({state} for task {task_name}; assuming task is alive.')
            return True

        tries += 1
        # info
        logger.error(f'Task {task_name} is not responding to queries. Tries: {tries}')

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


def wait_for_running_tasks_from_results(results: list[FireXAsyncResult], max_wait=2*60):
    sleep_between_iterations=0.05
    run_states = set(READY_STATES) | {STARTED, RETRY}
    running_tasks = []
    for result in results:
        if result.state in run_states and not result.fx_postrun_complete():
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
        running_tasks = [r for r in running_tasks if not r.fx_postrun_complete()]
        sleep_between_iterations = sleep_between_iterations * 1.01 \
            if sleep_between_iterations*1.01 < max_sleep else max_sleep

    if running_tasks:
        logger.error(f'The following tasks may still be running after task-wait timeout has expired:\n'
                     f'{get_tasks_names_from_results(running_tasks)}')


@send_block_task_states_to_caller_task
def wait_on_async_results(
    results: Union[FireXAsyncResult, Iterable[FireXAsyncResult]],
    max_wait=None,
    callbacks: Iterable[WaitLoopCallBack]=tuple(),
    sleep_between_iterations=0.05,
    check_task_worker_frequency=600,
    check_task_worker_timeout=180,
    check_task_worker_inspect_retry_timeout=30,
    fail_on_worker_failures=3,
    log_msg=True,
    **_kwargs,
):

    results = results or []

    if isinstance(results, AsyncResult):
        results = [results]

    max_sleep = sleep_between_iterations * 20 * 15  # Somewhat arbitrary
    failures = []
    start_time = time.monotonic()
    last_callback_time = {callback.func: start_time for callback in callbacks}
    for result in results:
        logging_name = result.log_name
        if log_msg:
            logger.debug(f'-> Waiting for {logging_name} to complete')

        try:
            task_worker_failures = 0
            last_dead_task_worker_check = time.monotonic()
            while not result.fx_ready():
                _check_for_failure_in_parents(result)

                current_time = time.monotonic()
                if max_wait and (current_time - start_time) > max_wait:
                    raise WaitOnChainTimeoutError(f'Result ID {logging_name} was not ready in {max_wait} seconds')

                # callbacks
                for callback in callbacks:
                    if (current_time - last_callback_time[callback.func]) > callback.frequency:
                        callback.func(**callback.kwargs)
                        last_callback_time[callback.func] = current_time

                # Check for dead workers
                if (
                    check_task_worker_frequency
                    and fail_on_worker_failures
                    and (current_time - last_dead_task_worker_check) > check_task_worker_frequency
                ):
                    alive = _is_worker_alive(
                        result,
                        check_task_worker_timeout,
                        check_task_worker_inspect_retry_timeout=check_task_worker_inspect_retry_timeout)

                    last_dead_task_worker_check = current_time
                    if not alive:
                        task_worker_failures += 1
                        logger.warning(f'Task {logging_name} appears to be a zombie.'
                                       f' Failures: {task_worker_failures}')
                        if task_worker_failures >= fail_on_worker_failures:
                            raise ChainInterruptedByZombieTaskException.from_ar(result)
                    else:
                        task_worker_failures = 0

                time.sleep(sleep_between_iterations)
                sleep_between_iterations = sleep_between_iterations * 1.01 \
                    if sleep_between_iterations*1.01 < max_sleep else max_sleep  # Exponential backoff

            # If failure happened in a chain, raise from the failing task within the chain
            _check_for_failure_in_parents(result)

            result_state = result.fx_state()
            if result_state == REVOKED:
                # Wait for revoked tasks to actually finish running
                # Somewhat long max_wait in case a task does work when revoked, like
                # killing a child run launched by the task.
                wait_for_running_tasks_from_results([result], max_wait=5*60)
                raise ChainRevokedException.from_ar(result)
            if result_state == PENDING:
                # Pending tasks can be in revoke list. State will still be PENDING.
                raise ChainRevokedPreRunException.from_ar(result)
            if result_state == FAILURE:
                raise ChainInterruptedException.from_ar(
                    result,
                    cause=result.result if isinstance(result.result, Exception) else None,
                )

        except (ChainRevokedException, ChainInterruptedException) as e:
            failures.append(e)

    if failures:
        if len(failures) == 1:
            to_raise = failures[0]
        else:
            to_raise = MultipleFailuresException(
                [e.task_id for e in failures if hasattr(e, 'task_id')],
                failures=failures)

        raise to_raise


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
def wait_for_any_results(
    results: Union[FireXAsyncResult, list[FireXAsyncResult]],
    max_wait=None,
    poll_max_wait=0.1,
    log_msg=False,
    callbacks: Iterable[WaitLoopCallBack] = tuple(),
    **kwargs,
):
    if isinstance(results, FireXAsyncResult):
        results = [results]

    _warn_on_never_callback(callbacks, poll_max_wait)

    start_time = time.time()

    logging_names = [f'-> {get_result_logging_name(result)}' for result in results]

    logger.debug('Waiting for any of the following tasks to complete:\n' + '\n'.join(logging_names))
    while len(results):
        if max_wait and max_wait < time.time() - start_time:
            raise WaitOnChainTimeoutError(f'Results {results} were still not ready after {max_wait} seconds')
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

    @staticmethod
    def from_ar(ar: FireXAsyncResult) -> 'ChainRevokedException':
        return ChainRevokedException(
            task_id=ar.id,
            task_name=ar.log_name,
        )


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

    @classmethod
    def from_ar(cls, ar: FireXAsyncResult, cause=None) -> 'ChainInterruptedException':
        return cls(
            task_id=ar.id,
            task_name=ar.log_name,
            cause=cause,
        )


class ChainInterruptedByZombieTaskException(ChainInterruptedException):
    def __str__(self):
        return super().__str__() + ': (zombie task)'


class MultipleFailuresException(ChainInterruptedException):
    MESSAGE = "The chain has been interrupted by multiple failing microservices: %s"

    def __init__(self, task_ids=('UNKNOWN',), failures=None):
        self.task_ids = task_ids
        self.failures = failures
        super(ChainInterruptedException, self).__init__()

    def __str__(self):
        return self.MESSAGE % self.task_ids


def get_task_results(results: dict) -> dict:
    try:
        return_keys = results[RETURN_KEYS_KEY]
    except KeyError:
        return {}
    else:
        return {k: results[k] for k in return_keys if k in results} if return_keys else {}


def get_tasks_inputs_from_result(results: dict) -> dict:
    # Returns a dict of key-value pairs of inputs passed down in the async result object
    try:
        return_keys = list(results[RETURN_KEYS_KEY])
    except KeyError:
        return results
    else:
        return_keys.append(RETURN_KEYS_KEY)
        return {k: v for k, v in results.items() if k not in return_keys}


def _get_all_results(result: AsyncResult,
                     all_results: dict,
                     return_keys_only=True,
                     merge_children_results=False,
                     exclude_id=None)-> None:

    if not result:
        return  # <-- Nothing to do

    if result.successful():
        ret = getattr(result, 'result', {}) or {}
    else:
        ret = {}

    if not return_keys_only and ret:
        # Inputs from child win, below
        all_results.update(get_tasks_inputs_from_result(ret))

    children = getattr(result, 'children', []) or [] if merge_children_results else []

    for child in children:
        if exclude_id and child and child.id == exclude_id:
            continue
        # Beware, recursion
        _get_all_results(child,
                         all_results=all_results,
                         return_keys_only=return_keys_only,
                         merge_children_results=merge_children_results,
                         exclude_id=exclude_id) # Unnecessary; exclude_id is usually a first-level child

    if ret:
        # Returns from the parent win
        all_results.update(get_task_results(ret))


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
                parent_id: Optional[str] = None,
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
    all_results = {}

    chain_members = [result]
    if extract_from_parents:
        current_node = result
        while current_node and current_node.id != parent_id and current_node.parent:
            chain_members.append(current_node.parent)
            current_node = current_node.parent

    while len(chain_members) > 1:
        # This means we have at least one parent to walk. Parents need to be walked first
        # because we want the latter services in a chain to override the earlier services
        # results. But we don't want to walk the child which is a member of the chain,
        # since this will be walked explicitly, so we exclude that.
        _get_all_results(result=chain_members.pop(),
                         all_results=all_results,
                         return_keys_only=return_keys_only,
                         merge_children_results=merge_children_results,
                         exclude_id=chain_members[-1].id)

    # After possibly walking parents, we get our results for "result" (and possibly all children)
    _get_all_results(result=result,
                     all_results=all_results,
                     return_keys_only=return_keys_only,
                     merge_children_results=merge_children_results)

    from firexkit.task import FireXTask
    if not return_keys or return_keys == FireXTask.DYNAMIC_RETURN or return_keys == (FireXTask.DYNAMIC_RETURN,):
        return all_results
    else:
        return results2tuple(all_results, return_keys)


def get_results_with_default(result: AsyncResult,
                             default=None,
                             error_msg: str = None,
                             **kwargs):
    if result.successful():
        return get_results(result, **kwargs)
    else:
        if isinstance(getattr(result, 'result'), Exception):
            exc_info = result.result
        else:
            exc_info = None
        error_msg = error_msg or f'Unable to get result from {result}'
        logger.error(error_msg, exc_info=exc_info)
        return default


#
# Returns the first exception that is not a "ChainInterruptedException"
# in the exceptions stack.
#
def first_non_chain_interrupted_exception(ex: BaseException) -> BaseException:
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


def get_all_children(
    node: FireXAsyncResult,
    skip_subtree_nodes: set[str],
) -> set[FireXAsyncResult]:
    """Iterate the children of node, skipping any nodes found in skip_subtree_nodes"""
    stack = deque([node])
    children : set[FireXAsyncResult] = set()
    timeout = 180
    timeout_time = time.monotonic() + timeout
    while stack:
        child : FireXAsyncResult = stack.popleft()
        if child.id not in skip_subtree_nodes:
            children.add(child)

            while time.monotonic() < timeout_time and not child.ready():
                logger.debug(f'{child.log_name} state is still {child.state}...sleeping and retrying')
                time.sleep(0.5)
            stack.extend(child for child in child.children or [])
    return children


def forget_subtree_results(
    head_node_result: FireXAsyncResult,
    skip_subtree_nodes: set[str],
    do_not_forget_ids: set[str],
):
    """Forget results of the subtree rooted at head_node_result, while skipping subtrees in skip_subtree_nodes,
    as well as nodes in do_not_forget_nodes
    """

    # Must get all the elements from the get_all_children() generator first!
    # We can't process the forgetting of one element at a time per iteration because the parent/children relationship
    # might be lost once we forget a node
    #
    subtree_nodes = get_all_children(head_node_result, skip_subtree_nodes)

    nodes_to_forget = {n for n in subtree_nodes if n.id not in do_not_forget_ids}

    msg = [f'Forgetting {len(nodes_to_forget)} results for tree root {get_result_logging_name(head_node_result)}']
    if skip_subtree_nodes:
        msg += [f'Skipping subtrees: {[ar_id for ar_id in skip_subtree_nodes]}']
    if do_not_forget_ids:
        msg += [f'Skipping nodes: {[ar_id for ar_id in do_not_forget_ids]}']
    logger.debug('\n'.join(msg))

    for node in nodes_to_forget:
        logger.debug(f'Forgetting result: {node.log_name}')
        node._cache = None
        node.backend.forget(node.id)
