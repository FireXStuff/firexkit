import json
import re
import sys
import logging
import os
import textwrap
from collections import OrderedDict
import inspect
from datetime import datetime
from functools import partial
from typing import Callable, Iterable, Optional, Union, Any
from urllib.parse import urljoin
from copy import deepcopy
import dataclasses

from celery.canvas import Signature, _chain
from celery.result import AsyncResult
from celery.states import REVOKED
from contextlib import contextmanager
from enum import Enum
from logging.handlers import WatchedFileHandler
from types import MethodType, MappingProxyType
from abc import abstractmethod
from celery.app.task import Task
from celery.local import PromiseProxy
from celery.signals import task_prerun, task_postrun, task_revoked
from celery.utils.log import get_task_logger, get_logger

from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_conversion import ConverterRegister
from firexkit.result import get_tasks_names_from_results, wait_for_any_results, \
    RETURN_KEYS_KEY, wait_on_async_results_and_maybe_raise, get_result_logging_name, ChainInterruptedException, \
    ChainRevokedException, last_causing_chain_interrupted_exception, \
    wait_for_running_tasks_from_results, WaitOnChainTimeoutError, get_results, \
    get_task_name_from_result, first_non_chain_interrupted_exception, forget_chain_results
from firexkit.resources import get_firex_css_filepath, get_firex_logo_filepath
from firexkit.firexkit_common import JINJA_ENV
import time

REPLACEMENT_TASK_NAME_POSTFIX = '_orig'
FIREX_REVOKE_COMPLETE_EVENT_TYPE = 'task-firex-revoked'

REDIS_DB_KEY_FOR_RESULTS_WITH_REPORTS = 'FIREX_RESULTS_WITH_REPORTS'
REDIS_DB_KEY_PREFIX_FOR_ENQUEUE_ONCE_UID = 'ENQUEUE_CHILD_ONCE_UID_'
REDIS_DB_KEY_PREFIX_FOR_ENQUEUE_ONCE_COUNT = 'ENQUEUE_CHILD_ONCE_COUNT_'
REDIS_DB_KEY_PREFIX_FOR_CACHE_ENABLED_UID = 'CACHE_ENABLED'

logger = get_task_logger(__name__)

################################################################
# Monkey patching
_orig_chain_apply_async__ = _chain.apply_async


@dataclasses.dataclass
class TaskEnqueueSpec:
    signature: Signature
    inject_abog: bool = True
    enqueue_opts: Optional[dict[str, Any]] = None


def _chain_apply_async(self: _chain, *args: tuple, **kwargs: dict) -> AsyncResult:
    try:
        chain_depth = 0
        for task in self.tasks:
            task.kwargs['chain_depth'] = chain_depth
            chain_depth += 1
    except AttributeError:
        pass
    return _orig_chain_apply_async__(self, *args, **kwargs)


_chain.apply_async = _chain_apply_async


class NotInCache(Exception):
    pass


class CacheResultNotPopulatedYetInRedis(NotInCache):
    pass


class UidNotInjectedInAbog(Exception):
    pass


class TaskContext:
    pass


class PendingChildStrategy(Enum):
    """
    Available strategies for handling remaining pending child tasks upon successful completion
    of the parent microservice.
    """

    Block = 0, "Default"
    Revoke = 1
    Continue = 2


class ReturnsCodingException(Exception):
    pass


class DyanmicReturnsNotADict(Exception):
    pass


class IllegalTaskNameException(Exception):
    pass


def create_collapse_ops(flex_collapse_ops_spec):
    from typing import Pattern

    if isinstance(flex_collapse_ops_spec, list):
        return flex_collapse_ops_spec

    result_ops = []
    if isinstance(flex_collapse_ops_spec, dict):
        for k, v in flex_collapse_ops_spec.items():
            op = {}
            if isinstance(k, str):
                op['relative_to_nodes'] = {'type': 'task_name', 'value': k}
            elif issubclass(type(k), Pattern):  # can't use isinstance till 3.6
                op['relative_to_nodes'] = {'type': 'task_name_regex', 'value': k.pattern}

            if isinstance(v, str):
                op['targets'] = [v]
            elif isinstance(v, list):
                op['targets'] = v
            elif isinstance(v, dict):
                # take targets, operation from value.
                op.update(v)

            if 'targets' in op:
                result_ops.append(op)
            else:
                # TODO: fail or ignore malformed specs?
                pass
    elif isinstance(flex_collapse_ops_spec, str):
        result_ops.append({'targets': [flex_collapse_ops_spec]})

    return result_ops


def expand_self_op():
    return {'operation': 'expand', 'targets': ['self']}


FLAME_COLLAPSE_KEY = '_default_display'


def flame_collapse_formatter(ops, task):
    filled_ops = []
    for op in ops:
        if 'targets' not in op or not isinstance(op['targets'], list):
            # Ignore malformed operations.
            continue
        filled_op = {
            # defaults.
            'relative_to_nodes': {'type': 'task_uuid', 'value': task.request.id},
            'source_node': {'type': 'task_uuid', 'value': task.request.id},
            'operation': 'collapse',
        }
        filled_op.update(op)
        filled_ops.append(filled_op)
    return filled_ops


def flame_collapse(flex_collapse_ops):
    static_ops = create_collapse_ops(flex_collapse_ops)
    return flame(FLAME_COLLAPSE_KEY, flame_collapse_formatter, data_type='object', bind=True, on_next=True,
                 on_next_args=[static_ops], decorator_name=flame_collapse.__name__)


def _default_flame_formatter(data):
    if data is None:
        return None
    if not isinstance(data, str):
        try:
            return json.dumps(data)
        except TypeError:
            return str(data)
    return data


def create_flame_config(existing_configs, formatter=_default_flame_formatter, data_type='html', bind=False,
                        on_next=False, on_next_args=()):
    return {'formatter': formatter,
            'data_type': data_type,
            'bind': bind,
            'on_next': on_next,
            'on_next_args': on_next_args,
            'order': max([c['order'] for c in existing_configs],
                         default=-1) + 1,
            }


def flame(flame_key=None, formatter=_default_flame_formatter, data_type='html', bind=False, on_next=False,
          on_next_args=(), decorator_name='flame'):
    def decorator(func):
        if type(func) is PromiseProxy:
            raise Exception(f"@{decorator_name} must be applied to a function (after @app.task) on {func.__name__}")

        undecorated = undecorate_func(func)
        if not hasattr(undecorated, 'flame_data_configs'):
            undecorated.flame_data_configs = OrderedDict()
        undecorated.flame_data_configs[flame_key] = create_flame_config(undecorated.flame_data_configs.values(),
                                                                        formatter, data_type, bind, on_next,
                                                                        on_next_args)
        return func

    return decorator


def _set_taskid_in_db_key(result: AsyncResult, db, db_key):
    db.set(db_key, result.id)
    logger.debug(f'Key {db_key} set to {result.id}')


class DictWillNotAllowWrites(dict):
    def __init__(self, _instrumentation_context=None, **kwargs):
        self.context = _instrumentation_context
        super().__init__(**kwargs)

    def warn(self):
        if self.context:
            try:
                self.context.send_task_instrumentation_event(abog_written_to=True)
            except Exception:
                logger.exception('Could not send instrumentation event')
        try:
            raise DeprecationWarning('DEPRECATION NOTICE: self.bog does not allow assignment. '
                                     'Please use self.abog.copy() to create a copy of the abog that you can write to. '
                                     'This will become an aborting failure starting January 1, 2023.')
        except DeprecationWarning as e:
            logger.exception(e)

    def __setitem__(self, *args, **kwargs):
        self.warn()
        super().__setitem__(*args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        self.warn()
        super().__delitem__(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self.warn()
        return super().pop(*args, **kwargs)

    def popitem(self):
        self.warn()
        return super().popitem()

    def clear(self):
        self.warn()
        super().clear()

    def setdefault(self, *args, **kwargs):
        self.warn()
        return super().setdefault(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.warn()
        super().update(*args, **kwargs)


def get_cache_enabled_uid_dbkey(cache_key_info: str) -> str:
    return f'{REDIS_DB_KEY_PREFIX_FOR_CACHE_ENABLED_UID}{cache_key_info}'


def get_enqueue_child_once_uid_dbkey(enqueue_once_key: str) -> str:
    return f'{REDIS_DB_KEY_PREFIX_FOR_ENQUEUE_ONCE_UID}{enqueue_once_key}'


def get_enqueue_child_once_count_dbkey(enqueue_once_key: str) -> str:
    return f'{REDIS_DB_KEY_PREFIX_FOR_ENQUEUE_ONCE_COUNT}{enqueue_once_key}'


def get_current_enqueue_child_once_uid_dbkeys(db) -> list[str]:
    return db.client.keys(get_enqueue_child_once_uid_dbkey('*'))


def get_current_cache_enabled_uid_dbkeys(db) -> list[str]:
    return db.client.keys(get_cache_enabled_uid_dbkey('*'))


def get_current_enqueue_child_once_uids(db) -> set[str]:
    """Returns a set of all task/result ids that were executed with enqueue_once"""

    # First, we need to find all the enqueue_once keys
    keys = get_current_enqueue_child_once_uid_dbkeys(db)
    # Then we get the task/result ids stored in those keys
    return {v.decode() for v in db.mget(keys)}


def get_current_cache_enabled_uids(db) -> set[str]:
    """Returns a set of all task/result ids whose tasks were cache-enabled"""

    # First, we need to find all the cache_enabled keys
    keys = get_current_cache_enabled_uid_dbkeys(db)
    # Then we get the task/result ids stored in those keys
    return {v.decode() for v in db.mget(keys)}


def add_task_result_with_report_to_db(db, result_id: str):
    """Append task id to the list of tasks with reports (e.g. tasks decorated with @email)"""
    db.client.rpush(REDIS_DB_KEY_FOR_RESULTS_WITH_REPORTS, result_id)


def get_current_reports_uids(db) -> set[str]:
    """Return the list of task/results ids for all tasks with reports (e.g. @email) executed so far"""
    return {v.decode() for v in db.client.lrange(REDIS_DB_KEY_FOR_RESULTS_WITH_REPORTS, 0, -1)}


class FireXTask(Task):
    """
    Task object that facilitates passing of arguments and return values from one task to another, to be used in chains
    """
    DYNAMIC_RETURN = '__DYNAMIC_RETURN__'
    RETURN_KEYS_KEY = RETURN_KEYS_KEY

    def __init__(self):

        check_name_for_override_posfix = getattr(self, 'check_name_for_override_posfix', True)
        if check_name_for_override_posfix and self.name and self.name.endswith(REPLACEMENT_TASK_NAME_POSTFIX):
            raise IllegalTaskNameException(f'Task names should never end with {REPLACEMENT_TASK_NAME_POSTFIX!r}')

        self.undecorated = undecorate(self)
        self.sig = inspect.signature(self.run)
        self._task_return_keys = self.get_task_return_keys()
        self._decorated_return_keys = getattr(self.undecorated, "_decorated_return_keys", tuple())
        if self._decorated_return_keys and self._task_return_keys:
            raise ReturnsCodingException(f"You can't specify both a @returns decorator and a returns in the app task for {self.name}")
        self.return_keys = self._decorated_return_keys or self._task_return_keys

        self._lagging_children_strategy = get_attr_unwrapped(self, 'pending_child_strategy', PendingChildStrategy.Block)

        super(FireXTask, self).__init__()

        self._in_required = None
        self._in_optional = None

        self._logs_dir_for_worker = None
        self._file_logging_dir_path = None
        self._task_logging_dirpath = None
        self._temp_loghandlers = None
        self.code_filepath = self.get_module_file_location()

        self._from_plugin = False

    @property
    def root_orig(self):
        """Return the very original `Task` that this `Task` had overridden.
        If this task has been overridden multiple times, this will return the very first/original task.
        Return `self` if the task was not overridden"""
        if hasattr(self, "orig"):
            return self.orig.root_orig
        return self

    def apply_async(self, *args, **kwargs):
        original_name = self.name
        if self.from_plugin and not original_name.endswith(REPLACEMENT_TASK_NAME_POSTFIX):
            # If the task is overridden, and is not an intermediate override, then
            # let's use the original name for serialization, in case that
            # override name isn't available in the execution context.
            # This can obviously be dangerous (but a risk we're deliberately taking)
            # since we bind the args/kwargs/runtime options with the ovverriden service
            # but might end up executing in a context that doesn't have it.
            self.name = self.root_orig.name_without_orig
        try:
            res = super(FireXTask, self).apply_async(*args, **kwargs)
        finally:
            # Restore the original name
            self.name = original_name
        return res

    def signature(self, *args, **kwargs):
        # We need to lookup the task, in case it was over-ridden by a plugin
        try:
            new_self = self.app.tasks[self.name]
        except Exception:
            # TODO: WTH
            # Some caching issue prevent tests from running
            # These tests should really run in forked processes (or use Celery PyTest fixtures)
            # Otherwise, seems that everything is global
            new_self = self.app.tasks[self.name]
        # Get the signature from the new_self
        return super(FireXTask, new_self).signature(*args, **kwargs)

    @contextmanager
    def task_context(self):
        try:
            self.context = TaskContext()
            self.initialize_context()
            yield
        finally:
            if hasattr(self, 'context'):
                del self.context

    @property
    def from_plugin(self):
        return self._from_plugin

    @property
    def task_label(self) -> str:
        """Returns a label for this task

        Examples:
            8345379a-e536-4566-b5c9-3d515ec5936a
            8345379a-e536-4566-b5c9-3d515ec5936a_2 (if it was the second retry)
            microservices.testsuites_tasks.CreateWorkerConfigFromTestsuites (if there was no request id yet)
        """
        label = str(self.request.id) if self.request.id else self.name
        label += '_%d' % self.request.retries if self.request.retries >= 1 else ''
        return label

    @property
    def request_soft_time_limit(self):
        return self.request.timelimit[1]

    @from_plugin.setter
    def from_plugin(self, value):
        self._from_plugin = value

    def initialize_context(self):
        self.context.enqueued_children = {}
        self.context.bog = None

        # Flame configs need to be on self.context b/c they write to flame_data_configs[k]['on_next'] for collapse ops.
        # Might make more sense to rework that to avoid flame data on context.
        self.context.flame_configs = (
            deepcopy(getattr(self.undecorated, "flame_data_configs", {}))
            # Overwrite @app.task(flame=...) driven-style when present over @flame() annotation style.
            | self.get_task_flame_configs()
        )

    def get_module_file_location(self):
        return sys.modules[self.__module__].__file__

    @classmethod
    def is_dynamic_return(cls, value):
        return hasattr(value, 'startswith') and value.startswith(cls.DYNAMIC_RETURN)

    def get_task_return_keys(self) -> tuple:
        task_return_keys = get_attr_unwrapped(self, 'returns', tuple())
        if task_return_keys:
            if isinstance(task_return_keys, str):
                task_return_keys = (task_return_keys, )

            explicit_keys = [k for k in task_return_keys if not self.is_dynamic_return(k)]
            if len(explicit_keys) != len(set(explicit_keys)):
                raise ReturnsCodingException("Can't have duplicate explicit return keys")

            if not isinstance(task_return_keys, tuple):
                task_return_keys = tuple(task_return_keys)
        return task_return_keys

    @classmethod
    def convert_returns_to_dict(cls, return_keys, result) -> dict:
        if type(result) != tuple and isinstance(result, tuple):
            # handle named tuples, they are a result, not all the results
            result = (result,)
        if not isinstance(result, tuple):
            # handle case of singular result
            result = (result, )

        if len(return_keys) != len(result):
            raise ReturnsCodingException('Expected %s keys in @returns' % len(return_keys))

        # time to process the multiple return values
        flat_results = OrderedDict()
        for k, v in zip(return_keys, result):
            if k == cls.DYNAMIC_RETURN:
                if not v:
                    continue
                if not isinstance(v, dict):
                    raise DyanmicReturnsNotADict('The value of the dynamic returns %s must be a dictionary.'
                                                 'Current return value %r is of type %s' % (k, v, type(v).__name__))
                flat_results.update(v)
            else:
                flat_results[k] = v
        result = flat_results
        _return_keys = list(result.keys())

        # Inject into the results the RETURN_KEYS
        if _return_keys:
            result[cls.RETURN_KEYS_KEY] = tuple(_return_keys)
        return result

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError('Tasks must define the run method.')

    @staticmethod
    def strip_orig_from_name(task_name):
        return re.sub(f"({REPLACEMENT_TASK_NAME_POSTFIX})*$", "", task_name)

    @staticmethod
    def get_short_name(task_name):
        # Task name of first task in chain. (I.E. 'task1' in module1.task1|module2.task2)
        return task_name.split('|')[0].split('.')[-1]

    @classmethod
    def get_short_name_without_orig(cls, task_name):
        return cls.strip_orig_from_name(cls.get_short_name(task_name))

    @property
    def name_without_orig(self):
        return self.strip_orig_from_name(self.name)

    @property
    def short_name(self):
        return self.get_short_name(self.name)

    @property
    def short_name_without_orig(self):
        return self.get_short_name_without_orig(self.name)

    @property
    def called_as_orig(self):
        return True if self.name.endswith(REPLACEMENT_TASK_NAME_POSTFIX) else False

    def has_report_meta(self) -> bool:
        """Does this task generate a report (e.g. decorated with @email)?"""
        return hasattr(self, 'report_meta')

    def add_task_result_with_report_to_db(self):
        """Maintain a list in the backend of all executed tasks that will generate reports"""
        return add_task_result_with_report_to_db(self.app.backend,  self.request.id)

    @abstractmethod
    def pre_task_run(self, extra_events: Optional[dict] = None):
        """
        Overrideable method to allow subclasses to do something with the
        BagOfGoodies before returning the results
        """

        if extra_events is None:
            extra_events = {}

        bound_args = self.bound_args
        default_bound_args = self.default_bound_args

        # Send a custom task-started-info event with the args
        if not self.request.called_directly:
            if self.has_report_meta():
                # If the task generates a report, append the task id
                # to the list in the backend of all executed tasks that  generate reports
                self.add_task_result_with_report_to_db()

            self.send_event('task-started-info',
                            firex_bound_args=convert_to_serializable(bound_args),
                            firex_default_bound_args=convert_to_serializable(default_bound_args),
                            called_as_orig=self.called_as_orig,
                            long_name=self.name_without_orig,
                            log_filepath=self.task_log_url,
                            from_plugin=self.from_plugin,
                            code_filepath=self.code_filepath,
                            retries=self.request.retries,
                            task_parent_id=self.request.parent_id,
                            **extra_events)
            self.send_firex_data(self.abog)

        # Print the pre-call header
        self.print_precall_header(bound_args, default_bound_args)
        self._log_soft_time_limit_override_if_applicable()

    def _log_soft_time_limit_override_if_applicable(self):
        if not self.request.called_directly:
            default_soft_time_limit = self.soft_time_limit
            request_soft_time_limit = self.request_soft_time_limit
            if default_soft_time_limit != request_soft_time_limit:
                logger.debug(f'This task default soft_time_limit of '
                             f'{default_soft_time_limit}{"s" if default_soft_time_limit is not None else ""} '
                             f'was over-ridden to {request_soft_time_limit}s')
            else:
                if default_soft_time_limit is not None:
                    logger.debug(f'This task soft_time_limit is {default_soft_time_limit}s')

    @abstractmethod
    def post_task_run(self, results, extra_events: Optional[dict] = None):
        """
        Overrideable method to allow subclasses to do something with the
        BagOfGoodies after the task has been run
        """

        if extra_events is None:
            extra_events = {}

        # No need to expose the RETURN_KEYS_KEY
        try:
            del results[RETURN_KEYS_KEY]
        except (TypeError, KeyError):
            pass

        # Print the post-call header
        self.print_postcall_header(results)

        # Send a custom task-succeeded event with the results
        if not self.request.called_directly:
            self.send_event('task-results', firex_result=convert_to_serializable(results), **extra_events)
            self.send_firex_data(self.abog)

    def print_precall_header(self, bound_args, default_bound_args):
        n = 1
        content = ''
        args_list = []
        for postfix, args in zip(['', ' (default)'], [bound_args, default_bound_args]):
            if args:
                for k, v in args.items():
                    args_list.append('  %d. %s: %r%s' % (n, k, v, postfix))
                    n += 1
        if args_list:
            content = 'ARGUMENTS\n' + '\n'.join(args_list)

        task_name = self.name
        if self.from_plugin:
            task_name += ' (PLUGIN)'

        logger.debug(banner('STARTED: %s' % task_name, content=content, length=100),
                     extra={'label': self.task_label, 'span_class': 'task_started'})

    def print_postcall_header(self, result):
        content = ''
        results_list = []
        if result:
            if isinstance(result, dict):
                n = 1
                for k, v in result.items():
                    results_list.append('  %d. %s: %r' % (n, k, v))
                    n += 1
            else:
                results_list.append(f'  {result!r}')
        if results_list:
            content = 'RETURNS\n' + '\n'.join(results_list)
        logger.debug(banner('COMPLETED: %s' % self.name, ch='*', content=content, length=100),
                     extra={'span_class': 'task_completed'})

    def __call__(self, *args, **kwargs):
        """
        This method should not be overridden since it provides the context (i.e., run state).
        Classes extending FireX should override the _call.
        """
        with self.task_context():
            return self._call(*args, **kwargs)

    def _call(self, *args, **kwargs):
        if not self.request.called_directly:
            self.add_task_logfile_handler()
        try:
            result = self._process_arguments_and_run(*args, **kwargs)

            if self._lagging_children_strategy is PendingChildStrategy.Block:
                try:
                    self.wait_for_children()
                except Exception as e:
                    logger.debug("The following exception was thrown (and caught) when wait_for_children was "
                                 "implicitly called by this task's base class:\n" + str(e))
            return result
        except Exception as e:
            self.handle_exception(e)
        finally:
            try:
                if self._lagging_children_strategy is not PendingChildStrategy.Continue:
                    self.revoke_nonready_children()
            finally:
                self.remove_task_logfile_handler()

    def handle_exception(self, e, logging_extra: dict=None, raise_exception=True):
        extra = {'span_class': 'exception'}
        if logging_extra:
            extra.update(logging_extra)

        if isinstance(e, ChainInterruptedException) or isinstance(e, ChainRevokedException):
            try:
                causing_e = last_causing_chain_interrupted_exception(e)
                exception_cause_uuid = causing_e.task_id
            except AttributeError:
                pass
            else:
                if exception_cause_uuid:
                    self.send_event('task-exception-cause', exception_cause_uuid=exception_cause_uuid)
        mssg = f'{type(e).__name__}'
        exception_string = str(e)
        if exception_string:
            mssg += f': {exception_string}'
        logger.error(mssg, exc_info=e, extra=extra)
        if raise_exception:
            raise e

    def _process_result(self, result, extra_events: Optional[dict] = None):
        # Need to update the dict with the results, if @results was used
        if isinstance(result, dict):
            self.context.bog.update(result)

        # run any post converters attached to this task
        converted = ConverterRegister.task_convert(task_name=self.name, pre_task=False, **self.bag.copy())
        self.context.bog.update(converted)

        if isinstance(result, dict):
            # update the results with changes from converters
            result = {k: v for k, v in self.bag.items() if k in result}

        # give sub-classes a chance to do something with the results
        self.post_task_run(result, extra_events=extra_events)

        return self.bag.copy()

    def convert_results_if_returns_defined_by_task_definition(self, result):
        # If @returns decorator was used, we don't need to convert since that's taken care by the decorator
        if not self._decorated_return_keys and self._task_return_keys:
            # This is only used if the @app.task(returns=) is used
            result = self.convert_returns_to_dict(self._task_return_keys, result)
        return result

    def _get_cache_key(self):
        # Need a sting hash of name + all_args
        return get_cache_enabled_uid_dbkey(str((self.name,) + tuple(sorted(self.all_args.items()))))

    def _cache_get(self, cache_key):
        cached_uuid = self.backend.get(cache_key)
        if cached_uuid is None:
            raise NotInCache()
        cached_uuid = cached_uuid.decode()
        logger.info(f'[Caching] found entry for key {cache_key!r} at {cached_uuid!r}')
        return cached_uuid

    def _cache_set(self, cache_key, uuid):
        # We need to just store a reference  to the uuid (no need to store the result again)
        logger.debug(f'[Caching] storing entry for key {cache_key!r} -> {uuid!r}')
        self.backend.set(cache_key, uuid)

    @classmethod
    def _retrieve_result_from_backend(cls, cached_uuid, secs_to_wait_for_cached_result: int=3):
        # Retrieve the result of the original cached uuid from the backend
        logger.info(f'Retrieving result for {cached_uuid}; might take up to {secs_to_wait_for_cached_result} seconds.')
        loop_start_time = current_time = time.time()
        while (current_time - loop_start_time) < secs_to_wait_for_cached_result:
            result = cls.app.backend.get_result(cached_uuid)
            if set(result.keys()) != {'hostname', 'pid'}:
                # When the result is not populated, we get a dict back with these two keys
                break # --< We're done
            else:
                logger.debug(f'Result not populated yet!')
                time.sleep(0.1)
                current_time = time.time()
        else:
            raise CacheResultNotPopulatedYetInRedis(f'result for {cached_uuid} not populated '
                                                    f'in redis after {secs_to_wait_for_cached_result}s')
        return result

    @classmethod
    def _run_from_cache(cls, cached_uuid):
        result = cls._retrieve_result_from_backend(cached_uuid)
        cleaned_result = cls.convert_cached_results(result)
        return cleaned_result

    @classmethod
    def convert_cached_results(cls, result):
        return_keys = result.get('__task_return_keys', ()) + ('__task_return_keys',)
        return {k: v for k, v in result.items() if k in return_keys}

    @property
    def default_use_cache(self):
        return getattr(self, 'use_cache', None)

    def is_cache_enabled(self):
        use_cache_value = self.default_use_cache
        if not self.request.called_directly:
            try:
                request_use_cache = self.request.properties['use_cache']
            except KeyError:
                pass
            else:
                if request_use_cache is not None:
                    if request_use_cache != self.default_use_cache:
                        logger.debug(f'use_cache default value of {self.default_use_cache!r} for task {self.name!r} '
                                     f'was overridden by enqueue to {request_use_cache!r}')
                    use_cache_value = request_use_cache
        return bool(use_cache_value)

    def _real_call_and_cache_set(self, cache_key):
        result = self.real_call()
        self._cache_set(cache_key, self.request.id)
        return result

    def cache_call(self):
        cache_key = self._get_cache_key()
        try:
            cached_uuid = self._cache_get(cache_key)
        except NotInCache:
            logger.debug(f'[Caching] No entry found for key {cache_key!r}')
            return self._real_call_and_cache_set(cache_key)
        else:
            try:
                result = self._run_from_cache(cached_uuid)
            except CacheResultNotPopulatedYetInRedis:
                logger.debug('[Caching] Cache result not populated yet in Redis. '
                             'Reverting to a real call')
                return self._real_call_and_cache_set(cache_key)
            else:
                return self._process_result(result,
                                            extra_events={'cached_result_from': cached_uuid})

    def real_call(self):
        result = super(FireXTask, self).__call__(*self.args, **self.kwargs)
        converted_result = self.convert_results_if_returns_defined_by_task_definition(result)
        return self._process_result(converted_result)

    def final_call(self, *args, **kwargs):
        if self.is_cache_enabled():
            return self.cache_call()
        else:
            return self.real_call()

    def _process_arguments_and_run(self, *args, **kwargs):
        # Organise the input args by creating a BagOfGoodies
        self.context.bog = BagOfGoodies(self.sig,
                                        args,
                                        kwargs,
                                        has_returns_from_previous_task=kwargs.get('chain_depth', 0) > 0)

        # run any "pre" converters attached to this task
        converted = ConverterRegister.task_convert(task_name=self.name, pre_task=True, **self.bag.copy())
        self.context.bog.update(converted)

        # give sub-classes a chance to do something with the args
        self.pre_task_run()

        return self.final_call(*self.args, **self.kwargs)

    def retry(self, *args, **kwargs):
        # Adds some logging to the original task retry

        if not self.request.called_directly:
            if self.request.retries == self.max_retries:
                logger.error(f'{self.short_name} failed all {self.max_retries} retry attempts')
            else:
                logger.warning(f'{self.short_name} failed and retrying {self.request.retries+1}/{self.max_retries}')
        super(FireXTask, self).retry(*args, **kwargs)

    @property
    def bag(self) -> MappingProxyType:
        return MappingProxyType(self.context.bog.get_bag())

    @property
    def required_args(self) -> list:
        """
        :return: list of required arguments to the microservice.
        """
        if self._in_required is None:
            self._in_required, self._in_optional = parse_signature(self.sig)
        return list(self._in_required)

    @property
    def optional_args(self) -> dict:
        """
        :return: dict of optional arguments to the microservice, and their values.
        """
        if self._in_required is None:
            self._in_required, self._in_optional = parse_signature(self.sig)
        return dict(self._in_optional)

    @staticmethod
    def _get_bound_args(sig, args, kwargs) -> dict:
        return sig.bind(*args, **kwargs).arguments

    @staticmethod
    def _get_default_bound_args(sig, bound_args) -> dict:
        # Find and store the remaining default arguments for debugging purposes
        default_bound_args = OrderedDict()
        params = sig.parameters
        for param in params.values():
            if param.name not in bound_args:
                if param.default == param.empty and param.kind == param.VAR_POSITIONAL:
                    # The param.default is set to param.empty in such cases, and need to be a tuple instead
                    default_bound_args[param.name] = tuple()
                else:
                    default_bound_args[param.name] = param.default
        return default_bound_args

    @property
    def args(self) -> list:
        return self.context.bog.args

    @property
    def kwargs(self) -> dict:
        return self.context.bog.kwargs

    @property
    def bound_args(self) -> dict:
        return self._get_bound_args(self.sig, self.args, self.kwargs)

    @property
    def default_bound_args(self) -> dict:
        return self._get_default_bound_args(self.sig, self.bound_args)

    def map_input_args_kwargs(self, *args, **kwargs) -> ((), {}):
        b = BagOfGoodies(self.sig,
                         *args,
                         **kwargs,
                         has_returns_from_previous_task=kwargs.get('chain_depth', 0) > 0)
        return b.args, b.kwargs

    def map_args(self, *args, **kwargs) -> dict:
        args, kwargs = self.map_input_args_kwargs(args, kwargs)
        bound_args = self._get_bound_args(self.sig, args, kwargs)
        default_bound_args = self._get_default_bound_args(self.sig, bound_args)
        return {**bound_args, **default_bound_args}

    @property
    def all_args(self) -> MappingProxyType:
        return MappingProxyType({**self.bound_args, **self.default_bound_args})

    @property
    def abog(self) -> DictWillNotAllowWrites:
        return DictWillNotAllowWrites(_instrumentation_context=self, **{**self.bag, **self.default_bound_args})

    @property
    def uid(self):
        try:
            return self.abog['uid']
        except KeyError:
            raise UidNotInjectedInAbog('Please ensure you either inject uid explicitly, '
                                       'or use either self.enqueue_child, self.enqueue_child_and_get_results, '
                                       'or self.enqueue_in_parallel')

    #######################
    # Enqueuing child tasks

    _STATE_KEY = 'state'
    _PENDING = 'pending'
    _UNBLOCKED = 'unblocked'

    @property
    def enqueued_children(self):
        return list(self.context.enqueued_children.keys())

    @property
    def pending_enqueued_children(self):
        return [child for child, result in self.context.enqueued_children.items() if
                result.get(self._STATE_KEY) == self._PENDING]

    @property
    def nonready_enqueued_children(self):
        return [child for child in self.context.enqueued_children if not child.ready()]

    def _add_enqueued_child(self, child_result):
        if child_result not in self.context.enqueued_children:
            self.context.enqueued_children[child_result] = {}

    def _remove_enqueued_child(self, child_result):
        if child_result in self.context.enqueued_children:
            del(self.context.enqueued_children[child_result])

    def _update_child_state(self, child_result, state):
        if child_result not in self.context.enqueued_children:
            self._add_enqueued_child(child_result)
        self.context.enqueued_children[child_result][self._STATE_KEY] = state

    def wait_for_any_children(self, pending_only=True, **kwargs):
        """Wait for any of the enqueued child tasks to run and complete"""
        child_results = self.pending_enqueued_children if pending_only else self.enqueued_children
        for completed_child_result in wait_for_any_results(child_results, **kwargs):
            self._update_child_state(completed_child_result, self._UNBLOCKED)
            yield completed_child_result

    def wait_for_children(self, pending_only=True, **kwargs):
        """Wait for all enqueued child tasks to run and complete"""
        child_results = self.pending_enqueued_children if pending_only else self.enqueued_children
        self.wait_for_specific_children(child_results=child_results, **kwargs)

    def forget_child_result(self,
                            child_result: AsyncResult,
                            do_not_forget_report_nodes: bool = True,
                            do_not_forget_enqueue_once_nodes: bool = True,
                            do_not_forget_cache_enabled_tasks_results: bool = True,
                            **kwargs):

        """Forget results of the tree rooted at the "chain-head" of child_result, while skipping subtrees in
        skip_subtree_nodes, as well as nodes in do_not_forget_nodes.

        If do_not_forget_report_nodes is True (default), do not forget report nodes (e.g. nodes decorated with @email)

        If do_not_forget_enqueue_once_nodes is True (default), do not forget subtrees rooted at nodes that were enqueued
        with enqueue_once

        If do_not_forget_cache_enabled_tasks_results is True (default), do not forget subtrees rooted at nodes that belong to
        services with cached=True
        """
        logger.debug('Forgetting results')

        skip_subtree_nodes: set[str] = set()

        if do_not_forget_enqueue_once_nodes:
            enqueue_once_subtree_nodes = get_current_enqueue_child_once_uids(self.backend)
            if enqueue_once_subtree_nodes:
                skip_subtree_nodes.update(enqueue_once_subtree_nodes)
                logger.debug(f'Enqueue once subtree nodes: {enqueue_once_subtree_nodes}')

        if do_not_forget_cache_enabled_tasks_results:
            cache_enabled_subtree_nodes = get_current_cache_enabled_uids(self.backend)
            if cache_enabled_subtree_nodes:
                skip_subtree_nodes.update(cache_enabled_subtree_nodes)
                logger.debug(f'Cache-enabled  subtree nodes: {cache_enabled_subtree_nodes}')

        report_nodes = None
        if do_not_forget_report_nodes:
            report_nodes = get_current_reports_uids(self.backend)
            if report_nodes:
                logger.debug(f'Report nodes: {report_nodes}')

        forget_chain_results(child_result,
                             skip_subtree_nodes=skip_subtree_nodes,
                             do_not_forget_nodes=report_nodes,
                             **kwargs)
        # Since we forget the child, we need to also remove it from the list of enqueued_children
        self._remove_enqueued_child(child_result)

    def forget_specific_children_results(self, child_results: list[AsyncResult], **kwargs):
        """Forget results for the explicitly provided child_results"""
        for child in child_results:
            self.forget_child_result(child, **kwargs)

    def forget_enqueued_children_results(self, **kwargs):
        """Forget results for the enqueued children of current task"""
        self.forget_specific_children_results(self.enqueued_children, **kwargs)

    def wait_for_specific_children(self, child_results, forget: bool = False, **kwargs):
        """Wait for the explicitly provided child_results to run and complete"""
        if isinstance(child_results, AsyncResult):
            child_results = [child_results]
        if child_results:
            logger.debug('Waiting for enqueued children: %r' % get_tasks_names_from_results(child_results))
            try:
                wait_on_async_results_and_maybe_raise(child_results, caller_task=self, **kwargs)
            finally:
                [self._update_child_state(child_result, self._UNBLOCKED) for child_result in child_results]
                if forget:
                    self.forget_specific_children_results(child_results)

    def enqueue_child(self, chain: Signature, add_to_enqueued_children: bool = True, block: bool = False,
                      raise_exception_on_failure: bool = None,
                      apply_async_epilogue: Callable[[AsyncResult], None] = None, apply_async_options=None,
                      forget: bool = False,
                      inject_uid: bool = False,
                      **kwargs) -> Optional[AsyncResult]:
        """Schedule a child task to run"""

        if raise_exception_on_failure is not None:
            if not block:
                raise ValueError('Cannot control exceptions on child failure if we don\'t block')
            # Only set it if not None, otherwise we want to leave the downstream default
            kwargs['raise_exception_on_failure'] = raise_exception_on_failure

        if apply_async_options is None:
            apply_async_options = dict()

        from firexkit.chain import InjectArgs, verify_chain_arguments

        if isinstance(chain, InjectArgs):
            return

        # Inject uid whenever possible
        if inject_uid:
            chain = InjectArgs(uid=self.uid) | chain

        verify_chain_arguments(chain)
        child_result = chain.apply_async(**apply_async_options)
        if apply_async_epilogue:
            apply_async_epilogue(child_result)
        if add_to_enqueued_children:
            self._update_child_state(child_result, self._PENDING)
        if block:
            try:
                wait_on_async_results_and_maybe_raise(results=child_result,
                                                      caller_task=self,
                                                      **kwargs)
            finally:
                if add_to_enqueued_children:
                    self._update_child_state(child_result, self._UNBLOCKED)
                if forget:
                    self.forget_specific_children_results([child_result])
        return child_result

    def enqueue_child_and_get_results(self,
                                      *args,
                                      return_keys: Union[str, tuple] = (),
                                      return_keys_only: bool = True,
                                      merge_children_results: bool = False,
                                      extract_from_parents: bool = True,
                                      **kwargs) -> Union[tuple, dict]:
        """Apply a ``chain``, and extract results from it.

        This is a better version of `enqueue_child_and_extract` where the defaults for
        `extract_from_children` and `extract_task_returns_only` defaults are more intuitive.
        Additionally, extract_from_parents defaults to True in this API.

        Note:
            This is shorthand for :meth:`enqueue_child` followed with :meth:`get_results`.

        Args:
            *args: Tuple of args required by  :meth:`enqueue_child`
            return_keys: A single return key string, or a tuple of keys to extract from the task results.
                The default value of :const:`None` will return a dictionary of key/value pairs for the returned results.
            return_keys_only: If set, only return results for keys specified by the tasks' `@returns`
                decorator or :attr:`returns` attribute, otherwise, returns will include key/value pairs from the BoG.
            merge_children_results: If set, extract and merge results from the children tasks as well.
            extract_from_parents: If set, will consider all results returned from tasks of the given chain (parents
                of the last task). Else will consider only results returned by the last task of the chain.
            **kwargs: Other options to :meth:`enqueue_child`

        Returns:
            The returns of `get_results`.

        See Also:
            get_results
        """

        return self.enqueue_child_and_extract(*args,
                                              return_keys=return_keys,
                                              extract_from_children=merge_children_results,
                                              extract_task_returns_only=return_keys_only,
                                              extract_from_parents=extract_from_parents,
                                              **kwargs)

    def enqueue_child_and_extract(self,
                                  *args,
                                  **kwargs) -> Union[tuple, dict]:
        """Apply a ``chain``, and extract results from it.

        See:
            _enqueue_child_and_extract
        """

        if kwargs.pop('enqueue_once_key', None):
            raise ValueError('Invalid argument. Use the enqueue_child_once_and_extract() api.')

        return self._enqueue_child_and_extract(*args, **kwargs)

    def _enqueue_child_and_extract(self,
                                   *args,
                                   return_keys: Union[str, tuple] = (),
                                   extract_from_children: bool = True,
                                   extract_task_returns_only: bool = False,
                                   enqueue_once_key: str = '',
                                   extract_from_parents: bool = True,
                                   forget: bool = False,
                                   **kwargs) -> Union[tuple, dict]:
        """Apply a ``chain``, and extract results from it.

        Note:
            This is shorthand for :meth:`enqueue_child` followed with :meth:`extract_and_filter`.

        Args:
            *args: Tuple of args required by  :meth:`enqueue_child`
            return_keys: A single return key string, or a tuple of keys to extract from the task results.
                The default value of :const:`None` will return a dictionary of key/value pairs for the returned results.
            extract_from_children: If set, extract and merge results from the children tasks as well.
            extract_task_returns_only: If set, only return results for keys specified by the tasks' `@returns`
                decorator or :attr:`returns` attribute, otherwise, returns will include key/value pairs from the BoG.
            enqueue_once_key: a string key, which, if set will be used to check if this task needs to be run,
                Use the enqueue_child_once_and_extract wrapper to set this. Should be unique per FireX run.
            extract_from_parents: If set, will consider all results returned from tasks of the given chain (parents
                of the last task). Else will consider only results returned by the last task of the chain.
                NOTE: Will not work on reconstituted AsyncResult objects, such as those sometimes created by
                the enqueue_once API.
            **kwargs: Other options to :meth:`enqueue_child`

        Returns:
            The returns of `extract_and_filter`.

        See Also:
            extract_and_filter
        """

        # Remove block from kwargs if it exists
        _block = kwargs.pop('block', True)
        if not _block:
            logger.warning(f'enqueue_child_and_extract ignored block={_block}, '
                           'since it needs to block in order to extract results')

        if not enqueue_once_key:
            result_promise = self.enqueue_child(*args, block=True, **kwargs)
        else:
            # Need to make sure task with this key is run only once
            result_promise = self.enqueue_child_once(*args,
                                                     enqueue_once_key=enqueue_once_key,
                                                     block=True,
                                                     **kwargs)

        results = get_results(result_promise,
                              return_keys=return_keys,
                              merge_children_results=extract_from_children,
                              return_keys_only=extract_task_returns_only,
                              extract_from_parents=extract_from_parents)

        if forget:
            self.forget_specific_children_results([result_promise])

        return results

    def enqueue_child_once(self, *args, enqueue_once_key, block=False, **kwargs):
        """See  :`meth:`enqueue_child_once_and_extract`
        """

        if self.request.retries > 0:
            # NOTE: We presume previous run of the enqueue service failed and needs to rerun, so we use a new key.
            # There is danger here: If we are retrying before originally enqueueing this service, it's possible that
            # a regular enqueue of this service from elsewhere is still running and the new enqueue with the new
            # key will clash with that one
            enqueue_once_key = f'{enqueue_once_key}_{self.request.retries}'
            logger.info(f'Enqueue once: set new enqueue key, since this is a retry ({enqueue_once_key})')

        enqueue_child_once_uid_dbkey = get_enqueue_child_once_uid_dbkey(enqueue_once_key)
        enqueue_child_once_count_dbkey = get_enqueue_child_once_count_dbkey(enqueue_once_key)

        num_runs_attempted = self.backend.client.incr(enqueue_child_once_count_dbkey)
        if int(num_runs_attempted) == 1:
            # This is the first attempt, enqueue the child
            apply_async_epilogue = partial(_set_taskid_in_db_key,
                                           db=self.backend,
                                           db_key=enqueue_child_once_uid_dbkey)
            return self.enqueue_child(*args,
                                      block=block,
                                      apply_async_epilogue=apply_async_epilogue,
                                      **kwargs)  # <-- Done!

        # Someone else is running this; wait for uuid to be set in the backend
        logger.info(f'Skipping enqueue of task with enqueue-once key {enqueue_once_key}; '
                    f'It\'s being enqueued by a different owner.')

        # Wait for task-id to show up in the backend
        sec_to_wait = 60
        logger.info(f'Checking for task-id; might take up to {sec_to_wait} seconds.')
        loop_start_time = current_time = time.time()
        while (current_time - loop_start_time) < sec_to_wait:
            uid_of_enqueued_task = self.backend.get(enqueue_child_once_uid_dbkey)
            if uid_of_enqueued_task:
                break  # <-- we are done!
            time.sleep(0.1)
            current_time = time.time()
        else:
            # This is unexpected, since we expect uuid to be set by whoever is enqueueing this
            raise WaitOnChainTimeoutError(f'Timed out waiting for task-id to be set.'
                                          f' (enqueue-once key: {enqueue_once_key})')

        task_uid = uid_of_enqueued_task.decode()
        logger.info(f'{enqueue_once_key} is enqueued with task-id: {task_uid}')
        self._send_flame_additional_child(task_uid)

        result = AsyncResult(task_uid)
        if block:
            logger.debug(f'Waiting for results of non-child task {get_result_logging_name(result)}')
            wait_on_async_results_and_maybe_raise(results=result, caller_task=self, **kwargs)
        return result

    def enqueue_child_once_and_extract(self,
                                       *args,
                                       enqueue_once_key: str,
                                       **kwargs) -> [tuple, dict]:
        """Apply a ``chain`` with a unique key only once per FireX run, and extract results from it.

        Note:
            This is like :meth:`enqueue_child_and_extract`, but it sets `enqueue_once_key`.
        """

        if kwargs.pop('extract_from_parents', False):
            raise ValueError('Unable to extract returns from parents when using enqueue_child_once.')

        return self._enqueue_child_and_extract(*args,
                                               enqueue_once_key=enqueue_once_key,
                                               extract_from_parents=False,
                                               **kwargs)

    def enqueue_in_parallel(self, chains, max_parallel_chains=15, wait_for_completion=True,
                            raise_exception_on_failure=False, **kwargs):
        """ This method executes the provided list of Signatures/Chains in parallel
        and returns the associated list of "async_result" objects.
        The results are returned in the same order as the input Signatures/Chains."""
        promises = []
        scheduled = []
        for c in chains:
            if len(scheduled) >= max_parallel_chains:
                # Reach the max allowed parallel chains, wait for one to complete before scheduling the next one.
                async_res = next(wait_for_any_results(scheduled, raise_exception_on_failure=raise_exception_on_failure))
                scheduled.remove(async_res)
            # Schedule the next child
            logger.debug(f'Enqueueing: {c.get_label()}')
            promise = self.enqueue_child(c, **kwargs)
            scheduled.append(promise)
            promises.append(promise)
        if wait_for_completion or raise_exception_on_failure:
            # Wait for all children to complete
            self.wait_for_specific_children(promises, raise_exception_on_failure=raise_exception_on_failure)
        return promises

    def enqueue_child_from_spec(self,
                                task_spec: TaskEnqueueSpec,
                                inject_args: Optional[dict] = None):
        enqueue_opts = task_spec.enqueue_opts or dict()
        chain = task_spec.signature
        args_to_inject = self.abog.copy() if task_spec.inject_abog else {}
        if inject_args:
            args_to_inject.update(inject_args)
        if args_to_inject:
            from firexkit.chain import InjectArgs
            chain = InjectArgs(**args_to_inject) | chain
        logger.debug(f'Enqueuing {task_spec}')
        self.enqueue_child(chain, **enqueue_opts)

    def revoke_nonready_children(self):
        nonready_children = self.nonready_enqueued_children
        if nonready_children:
            logger.info('Nonready children of current task exist.')
            revoked = [self.revoke_child(child_result) for child_result in nonready_children]
            wait_for_running_tasks_from_results([result for result_list in revoked for result in result_list])

    def revoke_child(self, result: AsyncResult, terminate=True, wait=False, timeout=None):
        name = get_result_logging_name(result)
        logger.debug('Revoking child %s' % name)
        result.revoke(terminate=terminate, wait=wait, timeout=timeout)
        revoked_results = [result]
        self._update_child_state(result, self._UNBLOCKED)
        logger.info(f'Revoked {name}')

        while result.parent:
            # Walk up the chain, since nobody is waiting on those tasks explicitly.
            result = result.parent
            if not result.ready():
                name = get_result_logging_name(result)
                logger.debug(f'Revoking parent {name}')
                result.revoke(terminate=terminate, wait=wait, timeout=timeout)
                revoked_results.append(result)
                logger.info(f'Revoked {name}')
        return revoked_results

    @property
    def root_logger(self):
        return logger.root

    @property
    def root_logger_file_handler(self):
        return [handler for handler in self.root_logger.handlers if isinstance(handler, WatchedFileHandler)][0]

    @property
    def worker_log_file(self):
        return self.root_logger_file_handler.baseFilename

    @property
    def file_logging_dirpath(self):
        if self._file_logging_dir_path:
            return self._file_logging_dir_path
        else:
            self._file_logging_dir_path = os.path.dirname(self.worker_log_file)
            return self._file_logging_dir_path

    @property
    def logs_dir_for_worker(self):
        if self._logs_dir_for_worker:
            return self._logs_dir_for_worker
        else:
            self._logs_dir_for_worker = os.path.dirname(self.file_logging_dirpath)
            return self._logs_dir_for_worker

    def get_task_logging_dirpath_from_request(self, request):
        # Sometimes self.request isn't populated correctly, so we need to use this version instead of the property
        if self._task_logging_dirpath:
            return self._task_logging_dirpath
        else:
            _task_logging_dirpath = os.path.join(self.file_logging_dirpath, request.hostname)
            if not os.path.exists(_task_logging_dirpath):
                os.makedirs(_task_logging_dirpath, exist_ok=True)
            self._task_logging_dirpath = _task_logging_dirpath
            return self._task_logging_dirpath

    @property
    def task_logging_dirpath(self):
        return self.get_task_logging_dirpath_from_request(request=self.request)

    @property
    def task_log_url(self):
        if self.app.conf.install_config.has_viewer():
            # FIXME: there must be a more direct way of getting this relative path.
            log_entry_rel_run_root = os.path.relpath(self.task_logfile, self.app.conf.logs_dir)
            return self.app.conf.install_config.get_log_entry_url(log_entry_rel_run_root)
        else:
            return self.task_logfile

    def get_task_logfile_from_request(self, request):
        # Sometimes self.request isn't populated correctly, so we need to use this version instead of the property
        return self.get_task_logfile(self.get_task_logging_dirpath_from_request(request=request), self.name, request.id)

    @property
    def task_logfile(self):
        return self.get_task_logfile_from_request(request=self.request)

    @classmethod
    def get_task_logfile(cls, task_logging_dirpath, task_name, uuid):
        return os.path.join(task_logging_dirpath, cls.get_task_logfilename(task_name, uuid))

    @staticmethod
    def get_task_logfilename(task_name, uuid):
        return '{}_{}.html'.format(task_name, str(uuid))

    @property
    def worker_log_url(self):
        worker_log_url = self.worker_log_file
        task_label = self.task_label
        if task_label:
            worker_log_url = urljoin(worker_log_url, f'#{task_label}')
        return worker_log_url

    def write_task_log_html_header(self):
        base_dir = self.task_logging_dirpath
        worker_name = self.request.hostname
        worker_hostname = worker_name.split('@', 1)[-1]
        html_header = JINJA_ENV.get_template('log_template.html').render(
            firex_stylesheet=get_firex_css_filepath(self.app.conf.resources_dir, relative_from=base_dir),
            logo=get_firex_logo_filepath(self.app.conf.resources_dir, relative_from=base_dir),
            firex_id=self.app.conf.uid,
            link_for_logo=self.app.conf.link_for_logo,
            header_main_title=self.name_without_orig,
            worker_log_url=os.path.relpath(self.worker_log_url, base_dir),
            worker_name=worker_name,
            worker_hostname=worker_hostname,
        )

        with open(self.task_logfile, 'w') as f:
            f.write(html_header)

    def add_task_logfile_handler(self):
        task_logfile = self.task_logfile

        if not os.path.isfile(task_logfile):
            self.write_task_log_html_header()

        self._temp_loghandlers = {}
        fh_root = logging.handlers.WatchedFileHandler(task_logfile, mode='a+')
        fh_root.setFormatter(self.root_logger_file_handler.formatter)
        self.root_logger.addHandler(fh_root)
        self._temp_loghandlers[self.root_logger] = fh_root

        task_logger = get_logger('celery.task')
        fh_task = logging.FileHandler(task_logfile, mode='a+')
        original_file_handler = [handler for handler in task_logger.handlers if
                                 isinstance(handler, WatchedFileHandler)][0]
        fh_task.setFormatter(original_file_handler.formatter)
        task_logger.addHandler(fh_task)
        self._temp_loghandlers[task_logger] = fh_task

    def remove_task_logfile_handler(self):
        if self._temp_loghandlers:
            for _logger, _handler in self._temp_loghandlers.items():
                _logger.removeHandler(_handler)

    def send_event(self, *args, **kwargs):
        if not self.request.called_directly:
            super(FireXTask, self).send_event(*args, **kwargs)

    def duration(self):
        return get_time_from_task_start(self.request.id, self.backend)

    def start_time(self):
        return get_task_start_time(self.request.id, self.backend)

    def get_task_flame_configs(self) -> OrderedDict:
        flame_value = get_attr_unwrapped(self, 'flame', None)
        task_flame_config = OrderedDict()
        if flame_value:
            if isinstance(flame_value, str):
                # Config is only a key name, fill in default config.
                task_flame_config = OrderedDict([(flame_value, create_flame_config([]))])
            elif isinstance(flame_value, list):
                # Config is a list of key names, each of which should get a default config.
                # Create list of default configs so that their order is set properly.
                default_flame_configs = []
                for _ in flame_value:
                    default_flame_configs.append(create_flame_config(default_flame_configs))
                # associated ordered default configs with key names from flame decorator.
                task_flame_config = OrderedDict([(key_name, default_flame_configs[i])
                                                 for i, key_name in enumerate(flame_value)])
            elif isinstance(flame_value, dict):
                task_flame_config = OrderedDict(flame_value)

        return task_flame_config

    def send_firex_data(self, data):
        if self.request.called_directly:
            return

        if getattr(self.context, 'flame_configs', False):
            def safe_format(formatter, fromatter_args, formatter_kwargs):
                try:
                    return formatter(*fromatter_args, **formatter_kwargs)
                except Exception as e:
                    logger.exception(e)
                    return None

            formatted_data = {}
            for flame_key, flame_config in self.context.flame_configs.items():
                # Data can be sent either because it is supplied in the input or if data was registered to be sent
                # 'on_next' during flame_data_config registration.
                if flame_key in data \
                        or flame_config['on_next'] \
                        or (flame_key is None or flame_key == '*'):
                    formatter_kwargs = {'task': self} if flame_config['bind'] else {}
                    if flame_key in data:
                        formatter_args = [data[flame_key]]
                    elif flame_config['on_next']:
                        formatter_args = flame_config['on_next_args']
                    elif flame_key is None or flame_key == '*':
                        # None means execute formatter with all data.
                        formatter_args = [data]
                    else:
                        formatter_args = []

                    format_result = safe_format(flame_config['formatter'], formatter_args, formatter_kwargs)
                    if format_result is not None:
                        formatted_data[flame_key] = {
                            'value': format_result,
                            'type': flame_config['data_type'],
                            'order': flame_config['order'],
                        }

            if formatted_data:
                self.send_firex_event_raw({'flame_data': formatted_data})
                sent_on_next_keys = [k for k, v in self.context.flame_configs.items()
                                     if v['on_next'] and k in formatted_data]
                for k in sent_on_next_keys:
                    self.context.flame_configs[k]['on_next'] = False

    def send_firex_event_raw(self, data):
        self.send_event('task-send-flame', **data)

    def update_firex_data(self, **kwargs):
        self.send_firex_data(kwargs)

    def send_firex_html(self, **kwargs):
        formatted_data = {flame_key: {'value': html_data,
                                      'type': 'html',
                                      'order': time.time()}
                          for flame_key, html_data in kwargs.items()}
        self.send_firex_event_raw({'flame_data': formatted_data})

    def send_display_collapse(self, task_uuid: str = None):
        """
            Collapse the current task (default), or collapse the task with the supplied UUID.
        """
        if task_uuid is None:
            task_uuid = self.request.id
        formatted_data = {
            FLAME_COLLAPSE_KEY: {
                'value': flame_collapse_formatter([{
                    'targets': ['self'],
                    'relative_to_nodes': {'type': 'task_uuid', 'value': task_uuid},
                }], self),
                'type': 'object',
                'order': time.time()}
        }
        self.send_firex_event_raw({'flame_data': formatted_data})


def undecorate_func(func):
    undecorated_func = func
    while True:
        try:
            undecorated_func = getattr(undecorated_func, '__wrapped__')
        except AttributeError:
            break
    return undecorated_func


def undecorate(task):
    """:return: the original function that was used to create a microservice"""
    undecorated_func = undecorate_func(task.run)
    if not inspect.ismethod(task.run) or inspect.ismethod(undecorated_func):
        return undecorated_func
    else:
        return MethodType(undecorated_func, task)


def task_prerequisite(pre_req_task: PromiseProxy, key: str=None, trigger: callable=bool) -> callable:
    """
    Register a prerequisite to a microservice.
        :param pre_req_task: microservice to be invoked if trigger returns False
        :param key: key in kwargs to pass to the trigger. If None, all kwargs are passed
        :param trigger: a function returning a bool. When False is returned, then pre_req_task is enqueued

    When adding multiple prerequisites, they must be added in reverse order (i.e. last one to run first)
    """
    if not callable(trigger):
        raise Exception("trigger must be a function returning a bool")

    def decorator(task_needing_pre_req: PromiseProxy)->PromiseProxy:
        def maybe_run(kwargs):
            if not key:
                trigger_arg = kwargs
            else:
                trigger_arg = kwargs.get(key)
            if not trigger(trigger_arg):
                from celery import current_task
                current_task.enqueue_child(pre_req_task.s(**kwargs), block=True)

        maybe_run.__name__ = task_needing_pre_req.__name__ + "Needs" + pre_req_task.__name__

        dependencies = ConverterRegister.list_converters(task_name=task_needing_pre_req.__name__, pre_task=True)
        ConverterRegister.register_for_task(task_needing_pre_req, True, *dependencies)(maybe_run)
        return task_needing_pre_req
    return decorator


def parse_signature(sig: inspect.Signature) -> (set, dict):
    """Parse the run function of a microservice and return required and optional arguments"""
    required = set()
    optional = {}
    for param in sig.parameters.values():
        if param.kind in [param.VAR_POSITIONAL, param.VAR_KEYWORD]:
            # skip *args, and **kwargs
            continue
        elif param.default is param.empty:
            required.add(param.name)
        else:
            optional[param.name] = param.default
    return required, optional


def get_attr_unwrapped(fun: callable, attr_name, *default_value):
    """
    Unwraps a function and returns an attribute of the root function
    """
    while fun:
        try:
            return getattr(fun, attr_name)
        except AttributeError:
            fun = getattr(fun, '__wrapped__', None)
    if default_value:
        return default_value[0]
    raise AttributeError(attr_name)


def is_jsonable(obj) -> bool:
    """Returns :const:`True` if the `obj` can be serialized via Json,
    otherwise returns :const:`False`
    """
    try:
        json.dumps(obj)
    except TypeError:
        return False
    else:
        return True

def _custom_serializers(obj) -> str:
    # This is primarily done to make root service "unsuccessful_services" visible in run.json
    if isinstance(obj, AsyncResult) and obj.failed():
        task_name = get_task_name_from_result(obj)
        if task_name:
            if isinstance(obj.result, Exception):
                failure = first_non_chain_interrupted_exception(obj.result)
            else:
                failure = obj.result
            return f'{task_name.split(".")[-1]} failed: {failure}'

    return None


def convert_to_serializable(obj, max_recursive_depth=10, _depth=0):

    if obj is None or isinstance(obj, (int, float, str, bool)):
        return obj

    if hasattr(obj, 'firex_serializable'):
        return obj.firex_serializable()

    if isinstance(obj, datetime):
        obj = obj.isoformat()

    if dataclasses.is_dataclass(obj):
        try:
            obj = dataclasses.asdict(obj)
        except TypeError:
            pass # e.g. enums

    if is_jsonable(obj):
        return obj

    # recursive reference guard.
    if _depth < max_recursive_depth:
        # Full object isn't jsonable, but some contents might be. Try walking the structure to get jsonable parts.
        if isinstance(obj, dict):
            return {
                convert_to_serializable(k, max_recursive_depth, _depth+1): convert_to_serializable(v, max_recursive_depth, _depth+1)
                for k, v in obj.items()
            }

        # Note that it's important this DOES NOT catch strings, and it won't since strings are jsonable.
        if isinstance(obj, Iterable):
            return [convert_to_serializable(e, max_recursive_depth, _depth+1) for e in obj]

    # Either input isn't walkable (i.e. dict or iterable), or we're too deep in the structure to keep walking.
    custom_serialized = _custom_serializers(obj)
    if custom_serialized is not None:
        return custom_serialized
    return repr(obj)


def banner(text, ch='=', length=78, content=''):
    if content:
        content = '\n'.join(['\n'.join(textwrap.wrap(line, width=length)) for line in content.split('\n')])
        content += '\n'
    spaced_text = '\n'.join(
        ['\n'.join(textwrap.wrap(line, width=length, drop_whitespace=False)) for line in text.split('\n')])
    return '\n' + ch * length + '\n' + spaced_text.center(length, ch) + '\n' + content + ch * length + '\n'


def get_starttime_dbkey(task_id):
    return task_id + '_starttime'


def get_task_start_time(task_id, backend):
    starttime_dbkey = get_starttime_dbkey(task_id)
    try:
        return float(backend.get(starttime_dbkey))
    except:
        return None


def get_time_from_task_start(task_id, backend):
    start_time = get_task_start_time(task_id, backend)
    if start_time:
        runtime = time.time() - start_time
        return runtime
    return None


@task_prerun.connect
def statsd_task_prerun(sender, task, task_id, args, kwargs, **donotcare):
    starttime_dbkey = get_starttime_dbkey(task_id)
    if not sender.backend.get(starttime_dbkey):
        sender.backend.set(starttime_dbkey, time.time())


def send_task_completed_event(task, task_id, backend):
    actual_runtime = get_time_from_task_start(task_id, backend)
    if actual_runtime is not None:
        task.send_event('task-completed', actual_runtime=convert_to_serializable(actual_runtime))


@task_postrun.connect
def statsd_task_postrun(sender, task, task_id, args, kwargs, **donotcare):
    send_task_completed_event(task, task_id, sender.backend)

    # Celery can send task-revoked event before task is completed, allowing other states (e.g. task-unblocked) to
    # be emitted after task-revoked. Sending another indicator of revoked here allows the terminal state to be
    # correctly captured by listeners, since task_postrun occurs when the task is _really_ complete.
    if task.AsyncResult(task_id).state == REVOKED:
        task.send_event(FIREX_REVOKE_COMPLETE_EVENT_TYPE)


@task_revoked.connect
def statsd_task_revoked(sender, request, terminated, signum, expired, **kwargs):
    send_task_completed_event(sender, request.id, sender.backend)
