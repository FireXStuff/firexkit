import typing
import enum
import socket
import pathlib
import psutil
import os

import pydantic
from celery.utils.log import get_task_logger
from celery import Celery
from celery.local import Proxy
from billiard.process import current_process
from firexkit.inspect import inspect_with_retry

logger = get_task_logger(__name__)



class BuiltinFireXQueues(enum.Enum):
    # values are worker_basenames
    SHUTDOWN = 'shutdown'
    MASTER = 'master'
    MASTER_WORKER = 'worker'
    MC = 'mc'

    def full_queue_name(self, hostname: str, spawn_group: typing.Optional[str]=None) -> str:
        return FireXWorkerId(
            worker_basename=self.worker_basename,
            spawn_group=spawn_group,
            hostname=hostname,
        ).as_str()

    @property
    def worker_basename(self) -> str:
        return self.value

    def __str__(self):
        return self.worker_basename



class FireXCeleryQueue(pydantic.BaseModel):
    name: str



class FireXWorkerId(pydantic.BaseModel, frozen=True):
    """
        e.g master:g1@yourhost

        Independent of a specific run.
    """
    worker_basename : str
    spawn_group: typing.Optional[str]
    hostname: str

    def as_str(self) -> str:
        return self.prefix_queue() + '@' + self.hostname

    def __str__(self) -> str:
        return self.as_str()

    def prefix_queue(self) -> str:
        return FireXWorkerId.create_prefix_queue(
            self.worker_basename, self.spawn_group
        )

    def as_no_spawn_group(self) -> 'FireXWorkerId':
        return self.model_copy(update=dict(spawn_group=None))

    def _as_other_worker(self, other_workerbase: typing.Union[BuiltinFireXQueues, str]) -> 'FireXWorkerId':
        return self.model_copy(
            update=dict(worker_basename=str(other_workerbase))
        )

    def as_sub_worker(self) -> 'FireXWorkerId':
        builtin_type = self._get_builtin_type()
        if builtin_type:
            if builtin_type == BuiltinFireXQueues.MC:
                # This seems weird? use pool stuff I guess?
                sub_worker_base = BuiltinFireXQueues.MASTER_WORKER.worker_basename
            elif builtin_type == BuiltinFireXQueues.MASTER:
                celery_proc_index = _get_cur_celery_proc_index()
                # celery_proc_index starts at 1, and we don't want the first
                # to have any suffix. Most masters only have 1 worker.
                if celery_proc_index is not None and celery_proc_index > 1:
                    sub_worker_base = f'worker-{celery_proc_index}'
                else:
                    sub_worker_base = BuiltinFireXQueues.MASTER_WORKER.worker_basename
            else:
                sub_worker_base = None
        else:
            sub_worker_base = None

        if sub_worker_base is None:
            return self
        return self._as_other_worker(sub_worker_base)

    def _get_builtin_type(self) -> typing.Optional[BuiltinFireXQueues]:
        for q in BuiltinFireXQueues:
            if self.worker_basename == q.worker_basename:
                return q
        import re
        if re.match(r'worker(-\d+)', self.worker_basename):
            return BuiltinFireXQueues.MASTER_WORKER
        return None

    def is_masters_worker(self) -> bool:
        return self._get_builtin_type() == BuiltinFireXQueues.MASTER_WORKER

    def is_master(self) -> bool:
        return self._get_builtin_type() == BuiltinFireXQueues.MASTER

    def is_mc(self) -> bool:
        return self._get_builtin_type() == BuiltinFireXQueues.MC

    def as_shutdown_queue(self) -> str:
        assert self._get_builtin_type() == BuiltinFireXQueues.MASTER, f'Only masters have shutdown queues, not {self._get_builtin_type()}'
        return str(self._as_other_worker(BuiltinFireXQueues.SHUTDOWN))

    def get_shutdown_file(self, logs_dir: str) -> str:
        return RunWorkerId.get_celery_logs_dir(logs_dir, f'{self}.shutdown')

    def on_cur_host(self) -> bool:
        return self.hostname == socket.gethostname()

    @staticmethod
    def create_prefix_queue(worker_basename: str, spawn_group: typing.Optional[str]) -> str:
        queue = worker_basename
        if spawn_group:
            queue += ':' + spawn_group
        return queue

    @staticmethod
    def master_id_prefix_queue(
        spawn_group: typing.Optional[str],
    ) -> str:
        return FireXWorkerId.create_prefix_queue(
            BuiltinFireXQueues.MASTER.worker_basename,
            spawn_group,
        )

    @staticmethod
    def master_id(
        spawn_group: typing.Optional[str],
        hostname: str,
    ) -> 'FireXWorkerId':
        return FireXWorkerId(
            worker_basename=BuiltinFireXQueues.MASTER.worker_basename,
            spawn_group=spawn_group,
            hostname=hostname,
        )

    @staticmethod
    def mc_id(hostname=None) -> 'FireXWorkerId':
        return FireXWorkerId(
            worker_basename=BuiltinFireXQueues.MC.worker_basename,
            spawn_group=None,
            hostname=hostname or socket.gethostname(),
        )

    @staticmethod
    def worker_id_str(
        worker_basename: typing.Union[str, BuiltinFireXQueues],
        spawn_group: typing.Optional[str],
        hostname: str,
    ) -> str:
        return FireXWorkerId(
            worker_basename=str(worker_basename),
            spawn_group=spawn_group,
            hostname=hostname,
        ).as_str()

    @staticmethod
    def worker_id_from_str(queue_str: typing.Union[str, 'FireXWorkerId']) -> 'FireXWorkerId':
        if isinstance(queue_str, FireXWorkerId):
            return queue_str
        assert '@' in queue_str, f'expected queue format <workername(:spawn_group)?@hostname>, received: {queue_str}'
        prefix_parts, hostname = queue_str.split('@', maxsplit=1)
        if ':' in prefix_parts:
            worker_basename, spawn_group = prefix_parts.split(':')
        else:
            worker_basename = prefix_parts
            spawn_group = None
        return FireXWorkerId(
            worker_basename=worker_basename,
            spawn_group=spawn_group,
            hostname=hostname,
        )

    @classmethod
    def is_mc_worker_id(cls, maybe_worker_id: typing.Union[str, None, 'FireXWorkerId']) -> bool:
        return bool(
            maybe_worker_id
            and str(maybe_worker_id).startswith(BuiltinFireXQueues.MC.worker_basename + '@')
        )

class _Unset:
    pass

_UNSET = _Unset()


class RunWorkerId(pydantic.BaseModel, frozen=True):
    worker_id: FireXWorkerId
    logs_dir: str

    def get_stdout_path(self) -> pathlib.Path:
        return pathlib.Path(self.get_celery_logs_dir(self.logs_dir, f'{self.worker_id}.stdout.txt'))

    def get_pid_file(self) -> pathlib.Path:
        return pathlib.Path(self._celery_pids_dir(self.logs_dir, f'{self.worker_id}.pid'))

    def get_pid(self) -> int:
        return _get_pid_from_file(self.get_pid_file())

    def get_pidfile_cmd_part(self, create_parent_dir=False) -> str:
        pidfile_path = self.get_pid_file()
        if create_parent_dir:
            pidfile_path.parent.mkdir(parents=True, exist_ok=True)
        return f'--pidfile={pidfile_path}'

    def terminate_pid_file(self, timeout=60, kill=False):
        logger.info(f'Terminating procs of {self.worker_id}')
        try:
            pid = self.get_pid()
        except Exception as e:
            logger.warning(e)
        else:
            try:
                logger.info(f'Terminating pid {pid}')
                p = psutil.Process(pid)
                if (
                    p.name() == 'celery'
                    and self.get_pidfile_cmd_part() in p.cmdline()
                ):
                    if kill:
                        p.terminate()
                    else:
                        p.kill()
                    p.wait(timeout=timeout)
            except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                self._kill_procs_by_pidfile_cmd_part()
            except psutil.Error as e:
                logger.warning(e)
            else:
                self._kill_procs_by_pidfile_cmd_part()

    def find_celery_procs_by_pidfile_cmdline(self) -> list[psutil.Process]:
        from firexapp.common import find_procs
        return find_procs('celery', cmdline_contains=self.get_pidfile_cmd_part())

    def _kill_procs_by_pidfile_cmd_part(self):
        for proc in self.find_celery_procs_by_pidfile_cmdline():
            logger.info(f'Killing pid {proc.pid}')
            try:
                proc.kill()
            except psutil.Error:
                logger.warning(f'Failed to kill pid {proc.pid}')

    @classmethod
    def terminate_localhost_pidfiles(cls, logs_dir: str, timeout=60):
        cur_hostname = socket.gethostname()
        try:
            for basename in os.listdir(cls._celery_pids_dir(logs_dir)):
                if '@' in basename and basename.endswith('.pid'):
                    worker_id = FireXWorkerId.worker_id_from_str(basename.removesuffix('.pid'))
                    if worker_id.hostname == cur_hostname:
                        run_worker_id = cls(worker_id=worker_id, logs_dir=logs_dir)
                        run_worker_id.terminate_pid_file(timeout=timeout)
                    else:
                        logger.warning(f'will not terminate worker pid on other host: {worker_id}')
        except OSError:
            logger.exception(f"Failed to terminate Celery pidfiles in {logs_dir}")

    @classmethod
    def _celery_pids_dir(cls, logs_dir: str, subpath: typing.Optional[str]=None) -> str:
        pids_dir = cls.get_celery_logs_dir(logs_dir, 'pids')
        if subpath:
            return os.path.join(pids_dir, subpath)
        return pids_dir

    @classmethod
    def get_celery_logs_dir(cls, logs_dir: str, subpath=None) -> str:
        from firexapp.submit.uid import Uid
        celery_log_dir = os.path.join(logs_dir, Uid.debug_dirname, 'celery')
        if subpath:
            return os.path.join(celery_log_dir, subpath)
        return celery_log_dir

    @classmethod
    def get_shutdown_ordered_worker_ids(cls, logs_dir: str) -> list['RunWorkerId']:
        #FIXME: do worker/master pairs (worker first), then mc last.
        ids = []
        for basename in os.listdir(cls._celery_pids_dir(logs_dir)):
            if '@' in basename and basename.endswith('.pid'):
                ids.append(
                    RunWorkerId(
                        worker_id=FireXWorkerId.worker_id_from_str(basename.removesuffix('.pid')),
                        logs_dir=logs_dir,
                    )
                )
        return ids

    @classmethod
    def find_celery_procs_by_logs_dir(cls, logs_dir: str) -> list:
        from firexapp.common import find_procs
        return find_procs('celery', cmdline_contains=f'--logfile={logs_dir}')


def _get_cur_celery_proc_index() -> typing.Optional[int]:
    celery_proc_identity = current_process()._identity
    # print(f'current proc _identity: {celery_proc_identity}')
    if isinstance(celery_proc_identity, int):
        celery_proc_index = celery_proc_identity
    elif (
        celery_proc_identity
        and len(celery_proc_identity) == 1
        and isinstance(celery_proc_identity[0], int)
    ):
        celery_proc_index = celery_proc_identity[0]
    else:
        celery_proc_index = None
    return celery_proc_index


def _get_pid_from_file(pid_file: pathlib.Path) -> int:
    try:
        with open(pid_file) as f:
            pid = f.read().strip()
    except FileNotFoundError:
        logger.warning(f'No pid file found in {pid_file}')
        raise
    else:
        if pid:
            return int(pid)
        else:
            raise AssertionError('no pid')


class FireXCeleryTask(pydantic.BaseModel):
    id: str
    worker_pid: typing.Optional[int] = None
    name: typing.Optional[str] = "UNKNOWN_TASK_NAME"
    time_start: typing.Optional[float] = None
    _proc: typing.Union[_Unset, psutil.Process, None] = _UNSET

    @pydantic.model_validator(mode='before')
    @classmethod
    def validate_str(cls, data: typing.Any) -> typing.Any:
        if isinstance(data, str):
            # Celery sometimes just has a UUID instead of the full dict, crazy.
            data = dict(id=data)
        return data

    def __str__(self) -> str:
        return f'{self.name}[{self.id}] (pid {self.worker_pid})'

    def worker_and_parent_procs_alive(self, on_error: typing.Optional[bool]=None) -> typing.Optional[bool]:
        try:
            worker_proc_alive = self.proc and self.proc.is_running()
            if not worker_proc_alive:
                return False
            worker_proc_parent = self.proc.parent()
            return bool(
                worker_proc_parent
                and worker_proc_parent.is_running()
            )
        except psutil.NoSuchProcess:
            return False
        except psutil.Error:
            return on_error

    @property
    def proc(self) -> typing.Optional[psutil.Process]:
        if isinstance(self._proc, _Unset):
            self._proc = self._get_proc()
        return self._proc

    def _get_proc(self) -> typing.Optional[psutil.Process]:
        if self.worker_pid is not None:
            proc = psutil.Process(self.worker_pid)
            if proc.name() == 'celery':
                return proc
            else:
                logger.warning(
                    f'Unexpected process name {proc.name()} for task worker pid {self.worker_pid}')

        return None

DEFAULT_INSPECT_TIMEOUT = 60

T = typing.TypeVar('T')
import dataclasses

class NoCeleryResponse(Exception):
    pass

@dataclasses.dataclass
class FireXCeleryInspector():
    celery_app: typing.Union[Proxy, Celery]
    retry_timeout: int=30
    inspect_timeout: int=DEFAULT_INSPECT_TIMEOUT
    retry_on_none_resp: typing.Optional[bool]=None
    raise_on_none_resp: bool=False

    def _resolve_retry_on_none_resp(self, retry_on_none_resp: typing.Optional[bool]):
        resolved_none_retry = True
        if retry_on_none_resp is None and self.retry_on_none_resp is None:
            pass # retry by default
        elif retry_on_none_resp is not None:
            resolved_none_retry = retry_on_none_resp
        elif self.retry_on_none_resp:
            resolved_none_retry = self.retry_on_none_resp
        return resolved_none_retry

    def _raw_inspect(
        self,
        inspect_method: str,
        resp_type: typing.Type[T],
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]],
        pattern: typing.Optional[str],
        timeout: typing.Optional[int],
        inspect_method_args: typing.Optional[typing.Iterable],
        retry_on_none_resp: typing.Optional[bool]=None,
    ) -> T:

        if worker_ids is None:
            destination = None
        else:
            destination = [str(w) for w in worker_ids]

        if timeout is None:
            timeout = self.inspect_timeout

        celery_resp = inspect_with_retry(
            inspect_method=inspect_method,
            method_args=inspect_method_args,
            destination=destination,
            celery_app=self.celery_app,
            timeout=timeout,
            pattern=pattern,
            inspect_retry_timeout=self.retry_timeout,
            retry_on_none_resp=self._resolve_retry_on_none_resp(retry_on_none_resp),
        )

        if not isinstance(celery_resp, resp_type):
            if celery_resp is not None:
                # None responses are "by design" from Celery APIs
                logger.warning(
                    f'Unexpected response from Celery when querying for {destination}.'
                    f' Required response type {resp_type}, found: {type(celery_resp)}')
            elif self.raise_on_none_resp:
                raise NoCeleryResponse(f'No Celery response running {inspect_method}({inspect_method_args}) with timeout {timeout} on workers {worker_ids}')
            result = resp_type()
        else:
            result = celery_resp

        return result

    def _raw_inspect_results_by_worker_ids(
        self,
        inspect_method: str,
        inspect_method_args: typing.Optional[typing.Iterable],
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]],
        pattern: typing.Optional[str],
        timeout: typing.Optional[int],
        retry_on_none_resp: typing.Optional[bool]=None,
    ) -> dict[FireXWorkerId, typing.Any]:

        results_by_worker_id_str = self._raw_inspect(
            inspect_method=inspect_method,
            inspect_method_args=inspect_method_args,
            resp_type=dict,
            worker_ids=worker_ids,
            timeout=timeout,
            pattern=pattern,
            retry_on_none_resp=retry_on_none_resp,
        )

        results_by_wid = {}
        for wid_str, wid_results in results_by_worker_id_str.items():
            if wid_str and '@' in wid_str:
                results_by_wid[FireXWorkerId.worker_id_from_str(wid_str)] = wid_results
            else:
                logger.warning(f'Unknown worker ID from Celery: {wid_str}')

        return results_by_wid

    def _raw_inspect_tasks_by_worker_ids(
        self,
        inspect_method: str,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]],
        timeout: typing.Optional[int],
        pattern: typing.Optional[str],
        inspect_method_args: typing.Optional[typing.Iterable]=None,
        retry_on_none_resp: typing.Optional[bool]=None,
    ) -> dict[FireXWorkerId, list[FireXCeleryTask]]:
        assert inspect_method in [
            'active', 'reserved', 'scheduled', 'query_task', 'active_queues',
            'revoked',
        ], f'unexpected inspect method: {inspect_method}'

        task_dicts_by_wid = self._raw_inspect_results_by_worker_ids(
            inspect_method=inspect_method,
            inspect_method_args=inspect_method_args,
            worker_ids=worker_ids,
            pattern=pattern,
            timeout=timeout,
            retry_on_none_resp=retry_on_none_resp,
        )

        tasks_by_wid : dict[FireXWorkerId, list[FireXCeleryTask]] = {}
        for wid, task_dicts in task_dicts_by_wid.items():
            tasks = []
            for task_dict in task_dicts:
                try:
                    task = FireXCeleryTask.model_validate(task_dict)
                except pydantic.ValidationError as e:
                    logger.warning(f'Failed to create firex task for {inspect_method} due to {e}, from {task_dict}')
                else:
                    tasks.append(task)

            tasks_by_wid[wid] = tasks

        return tasks_by_wid

    def _get_single_worker_inspect_tasks(
        self,
        inspect_method: str,
        worker_id: FireXWorkerId,
        timeout: typing.Optional[int],
    ) -> list['FireXCeleryTask']:

        tasks_by_worker_strs = self._raw_inspect_tasks_by_worker_ids(
            inspect_method,
            worker_ids=[worker_id],
            timeout=timeout,
            pattern=None,
        )
        if worker_id not in tasks_by_worker_strs:
            logger.warning(f'{worker_id} missing from Celery {inspect_method} response.')
            tasks = []
        else:
            tasks = tasks_by_worker_strs[worker_id]

        return tasks

    def get_single_worker_active_tasks(
        self,
        worker_id: FireXWorkerId,
        timeout: typing.Optional[int]=None,
    ) -> list['FireXCeleryTask']:
        return self._get_single_worker_inspect_tasks('active', worker_id, timeout)

    def get_single_worker_reserved_tasks(
        self,
        worker_id: FireXWorkerId,
        timeout: typing.Optional[int]=None,
    ) -> list['FireXCeleryTask']:
        return self._get_single_worker_inspect_tasks('reserved', worker_id, timeout)

    def get_single_worker_scheduled_tasks(
        self,
        worker_id: FireXWorkerId,
        timeout: typing.Optional[int]=None,
    ) -> list['FireXCeleryTask']:
        return self._get_single_worker_inspect_tasks('scheduled', worker_id, timeout)

    def get_active_tasks_by_worker_id(
        self,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
        pattern: typing.Optional[str]=None,
        timeout: typing.Optional[int]=None,
    ) -> dict['FireXWorkerId', list[FireXCeleryTask]]:
        return self._raw_inspect_tasks_by_worker_ids(
            'active',
            worker_ids=worker_ids,
            timeout=timeout,
            pattern=pattern,
        )

    def get_active_tasks(
        self,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
    ) -> list[FireXCeleryTask]:
        active_tasks : list[FireXCeleryTask] = []
        for worker_active_tasks in self.get_active_tasks_by_worker_id(worker_ids=worker_ids).values():
            active_tasks += worker_active_tasks
        return active_tasks

    def get_scheduled_tasks_by_worker_id(
        self,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
        pattern: typing.Optional[str]=None,
        timeout: typing.Optional[int]=None,
    ) -> dict['FireXWorkerId', list[FireXCeleryTask]]:
        return self._raw_inspect_tasks_by_worker_ids(
            'scheduled',
            worker_ids=worker_ids,
            timeout=timeout,
            pattern=pattern,
        )

    def get_reserved_tasks_by_worker_id(
        self,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
        pattern: typing.Optional[str]=None,
        timeout: typing.Optional[int]=None,
    ) -> dict['FireXWorkerId', list[FireXCeleryTask]]:
        return self._raw_inspect_tasks_by_worker_ids(
            'reserved',
            worker_ids=worker_ids,
            timeout=timeout,
            pattern=pattern,
        )

    def get_single_worker_active_queues(
        self,
        worker_id: FireXWorkerId,
        timeout: typing.Optional[int]=None,
    ) -> list[FireXCeleryQueue]:

        return self.get_active_queues_worker_id(
            worker_ids=[worker_id],
            timeout=timeout,
        ).get(worker_id, [])

    def get_active_queues_worker_id(
        self,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
        timeout: typing.Optional[int]=None,
    ) -> dict['FireXWorkerId', list[FireXCeleryQueue]]:
        queue_dicts_by_wid = self._raw_inspect_results_by_worker_ids(
            inspect_method='active_queues',
            inspect_method_args=None,
            worker_ids=worker_ids,
            pattern=None,
            timeout=timeout,
        )

        tasks_by_wid : dict[FireXWorkerId, list[FireXCeleryQueue]] = {}
        for wid, queue_dicts in queue_dicts_by_wid.items():
            queues = []
            for queue_dict in queue_dicts:
                try:
                    queue = FireXCeleryQueue.model_validate(queue_dict)
                except pydantic.ValidationError as e:
                    logger.warning(f'Failed to create firex queue due to {e}, from {queue_dict}')
                else:
                    queues.append(queue)

            tasks_by_wid[wid] = queues

        return tasks_by_wid

    def get_revoked_tasks(
        self,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
        timeout: typing.Optional[int]=None,
    ) -> list[FireXCeleryTask]:
        revoked_by_worker = self._raw_inspect_tasks_by_worker_ids(
            'revoked',
            worker_ids=worker_ids,
            timeout=timeout,
            pattern=None,
            retry_on_none_resp=False,
        )
        return _flatten(
            [revoked_tasks for revoked_tasks in revoked_by_worker.values()]
        )

    def get_task(
        self,
        task_uuid: str,
        worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]],
        timeout: typing.Optional[int]=None,
        pattern: typing.Optional[str]=None,
    ) -> typing.Optional[FireXCeleryTask]:

        tasks_by_worker_id = self._raw_inspect_tasks_by_worker_ids(
            'query_task',
            inspect_method_args=[task_uuid],
            worker_ids=worker_ids,
            timeout=timeout,
            pattern=pattern,
        )

        tasks = []
        for worker_tasks in tasks_by_worker_id.values():
            tasks += worker_tasks
        if not tasks:
            return None
        if len(tasks) > 1:
            logger.error(f'Found more than one task with ID {task_uuid}: {tasks}')
        return tasks[0]


def _flatten(l):
    return [item for sublist in l for item in sublist]