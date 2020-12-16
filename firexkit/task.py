import json
import re
import sys
import logging
import os
import textwrap
from collections import OrderedDict
import inspect
from typing import Callable, Iterable, Optional
from urllib.parse import urljoin

from celery.canvas import Signature
from celery.result import AsyncResult
from contextlib import contextmanager
from enum import Enum
from logging.handlers import WatchedFileHandler
from types import MethodType
from abc import abstractmethod
from celery.app.task import Task
from celery.local import PromiseProxy
from celery.signals import task_prerun, task_postrun, task_revoked
from celery.utils.log import get_task_logger, get_logger
from firexkit.revoke import revoke_recursively
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_conversion import ConverterRegister
from firexkit.result import get_tasks_names_from_results, wait_for_any_results, \
    RETURN_KEYS_KEY, wait_on_async_results_and_maybe_raise, get_result_logging_name, ChainInterruptedException, \
    ChainRevokedException
from firexkit.resources import get_firex_css_filepath, get_firex_logo_filepath, JINJA_ENV
import time

logger = get_task_logger(__name__)


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


class FireXTask(Task):
    """
    Task object that facilitates passing of arguments and return values from one task to another, to be used in chains
    """
    DYNAMIC_RETURN = '__DYNAMIC_RETURN__'
    RETURN_KEYS_KEY = RETURN_KEYS_KEY

    def __init__(self):
        self.undecorated = undecorate(self)
        self.sig = inspect.signature(self.run)
        self._task_return_keys = self.get_task_return_keys()
        self._decorated_return_keys = getattr(self.undecorated, "_decorated_return_keys", tuple())
        if self._decorated_return_keys and self._task_return_keys:
            raise ReturnsCodingException("You can't specify both a @returns decorator and a returns in the app task")
        self.return_keys = self._decorated_return_keys or self._task_return_keys

        self._lagging_children_strategy = get_attr_unwrapped(self, 'pending_child_strategy', PendingChildStrategy.Block)

        super(FireXTask, self).__init__()

        self._in_required = None
        self._in_optional = None

        self._file_logging_dir_path = None
        self._task_logging_dirpath = None
        self._temp_loghandlers = None
        self.code_filepath = self.get_module_file_location()

        self._from_plugin = False

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
        return re.sub("(_orig)*$", "", task_name)

    @staticmethod
    def get_short_name(task_name):
        # Task name of first task in chain. (I.E. 'task1' in module1.task1|module2.task2)
        return task_name.split('|')[0].split('.')[-1]

    @property
    def name_without_orig(self):
        return self.strip_orig_from_name(self.name)

    @property
    def short_name(self):
        return self.get_short_name(self.name)

    @property
    def short_name_without_orig(self):
        return self.strip_orig_from_name(self.short_name)

    @property
    def called_as_orig(self):
        return True if self.name.endswith('_orig') else False

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
            self.send_event('task-started-info',
                            firex_bound_args=convert_to_serializable(bound_args),
                            firex_default_bound_args=convert_to_serializable(default_bound_args),
                            called_as_orig=self.called_as_orig,
                            long_name=self.name_without_orig,
                            log_filepath=self.task_log_url,
                            from_plugin=self.from_plugin,
                            code_filepath=self.code_filepath,
                            retries=self.request.retries,
                            **extra_events)

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
                results_list.append('  %r' % result)
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
                    self.revoke_pending_children()
            finally:
                self.remove_task_logfile_handler()

    def handle_exception(self, e, logging_extra=None, raise_exception=True):
        if isinstance(e, ChainInterruptedException) or isinstance(e, ChainRevokedException):
            try:
                exception_cause_uuid = e.task_id
            except AttributeError:
                pass
            else:
                if exception_cause_uuid:
                    self.send_event('task-exception-cause', exception_cause_uuid=exception_cause_uuid)
        mssg = f'{type(e).__name__}'
        exception_string = str(e)
        if exception_string:
            mssg += f': {exception_string}'
        logger.error(mssg, exc_info=e, extra=logging_extra)
        if raise_exception:
            raise e

    def _process_arguments_and_run(self, *args, **kwargs):
        # Organise the input args by creating a BagOfGoodies
        self.context.bog = BagOfGoodies(self.sig, args, kwargs)

        # run any "pre" converters attached to this task
        converted = ConverterRegister.task_convert(task_name=self.name, pre_task=True, **self.bag)
        self.context.bog.update(converted)

        # give sub-classes a chance to do something with the args
        self.pre_task_run()

        result = super(FireXTask, self).__call__(*self.args, **self.kwargs)

        if not self._decorated_return_keys and self._task_return_keys:
            result = self.convert_returns_to_dict(self._task_return_keys, result)

        # Need to update the dict with the results, if @results was used
        if isinstance(result, dict):
            self.context.bog.update(result)

        # run any post converters attached to this task
        converted = ConverterRegister.task_convert(task_name=self.name, pre_task=False, **self.bag)
        self.context.bog.update(converted)

        if isinstance(result, dict):
            # update the results with changes from converters
            result = {k: v for k, v in self.bag.items() if k in result}

        # give sub-classes a change to do something with the results
        self.post_task_run(result)

        return self.bag

    def retry(self, *args, **kwargs):
        # Adds some logging to the original task retry

        if not self.request.called_directly:
            if self.request.retries == self.max_retries:
                logger.error(f'{self.short_name} failed all {self.max_retries} retry attempts')
            else:
                logger.warning(f'{self.short_name} failed and retrying {self.request.retries+1}/{self.max_retries}')
        super(FireXTask, self).retry(*args, **kwargs)

    @property
    def bag(self) -> dict:
        return self.context.bog.get_bag()

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
        b = BagOfGoodies(self.sig, *args, **kwargs)
        return b.args, b.kwargs

    def map_args(self, *args, **kwargs) -> dict:
        args, kwargs = self.map_input_args_kwargs(args, kwargs)
        bound_args = self._get_bound_args(self.sig, args, kwargs)
        default_bound_args = self._get_default_bound_args(self.sig, bound_args)
        return {**bound_args, **default_bound_args}

    @property
    def all_args(self) -> dict:
        return {**self.bound_args, **self.default_bound_args}

    @property
    def abog(self) -> dict:
        return {**self.bag, **self.all_args}

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

    def _add_enqueued_child(self, child_result):
        if child_result not in self.context.enqueued_children:
            self.context.enqueued_children[child_result] = {}

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

    def wait_for_specific_children(self, child_results, **kwargs: dict):
        """Wait for the explicitly provided child_results to run and complete"""
        if child_results:
            logger.debug('Waiting for enqueued children: %r' % get_tasks_names_from_results(child_results))
            try:
                wait_on_async_results_and_maybe_raise(child_results, caller_task=self, **kwargs)
            finally:
                [self._update_child_state(child_result, self._UNBLOCKED) for child_result in child_results]

    def enqueue_child(self, chain: Signature, add_to_enqueued_children: bool = True, block: bool = False,
                      raise_exception_on_failure: bool = None,
                      apply_async_epilogue: Callable[[AsyncResult], None] = None, apply_async_options=None,
                      **kwargs) -> AsyncResult:
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
        return child_result

    def enqueue_in_parallel(self, chains, max_parallel_chains=15, wait_for_completion=True,
                            raise_exception_on_failure=False):
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
            promise = self.enqueue_child(c)
            scheduled.append(promise)
            promises.append(promise)
        if wait_for_completion or raise_exception_on_failure:
            # Wait for all children to complete
            self.wait_for_specific_children(promises, raise_exception_on_failure=raise_exception_on_failure)
        return promises

    def revoke_pending_children(self, **kwargs):
        pending_children = self.pending_enqueued_children
        if pending_children:
            logger.info('Pending children of current task exist.')
            [self.revoke_child(child_result, **kwargs) for child_result in pending_children]

    def revoke_child(self, result: AsyncResult, **kwargs):
        logger.debug('Revoking child %s' % get_result_logging_name(result))
        revoke_recursively(result, **kwargs)
        self._update_child_state(result, self._UNBLOCKED)

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
    def task_logging_dirpath(self):
        if self._task_logging_dirpath:
            return self._task_logging_dirpath
        else:
            _task_logging_dirpath = self.get_task_logging_dirpath(self.file_logging_dirpath, self.request.hostname)
            if not os.path.exists(_task_logging_dirpath):
                os.makedirs(_task_logging_dirpath, mode=0o777, exist_ok=True)
            self._task_logging_dirpath = _task_logging_dirpath
            return self._task_logging_dirpath

    @staticmethod
    def get_task_logging_dirpath(file_logging_dirpath, hostname):
        return os.path.join(file_logging_dirpath, hostname)

    @property
    def task_log_url(self):
        if self.app.conf.install_config and self.app.conf.install_config.has_viewer():
            # FIXME: there must be a more direct way of getting this relative path.
            log_entry_rel_run_root = os.path.relpath(self.task_logfile, self.app.conf.logs_dir)
            return self.app.conf.install_config.get_log_entry_url(log_entry_rel_run_root)
        else:
            return self.task_logfile

    @property
    def task_logfile(self):
        return self.get_task_logfile(self.task_logging_dirpath, self.name, self.request.id)

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
        html_header = JINJA_ENV.get_template('log_template.html').render(
            firex_stylesheet=get_firex_css_filepath(self.app.conf.resources_dir, relative_from=base_dir),
            logo=get_firex_logo_filepath(self.app.conf.resources_dir, relative_from=base_dir),
            firex_id=self.app.conf.uid,
            link_for_logo=self.app.conf.link_for_logo,
            header_main_title=self.name_without_orig,
            worker_log_url=os.path.relpath(self.worker_log_url, base_dir),
            worker_name=self.request.hostname,
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


def parse_signature(sig: inspect.Signature)->(set, dict):
    """Parse the run function of a microservice and return required and optional arguments"""
    required = set()
    optional = {}
    for k, v in sig.parameters.items():
        default_value = v.default
        if default_value is not inspect.Signature.empty:
            optional[k] = default_value
        else:
            required.add(k)
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


def convert_to_serializable(obj, depth=0):
    if is_jsonable(obj):
        return obj

    # recursive reference guard.
    if depth < 10:
        # Full object isn't jsonable, but some contents might be. Try walking the structure to get jsonable parts.
        if isinstance(obj, dict):
            return {k: convert_to_serializable(v, depth+1) for k, v in obj.items()}

        # Note that it's important this DOES NOT catch strings, and it won't since strings are jsonable.
        if isinstance(obj, Iterable):
            return [convert_to_serializable(e, depth+1) for e in obj]

    # Either input isn't walkable (i.e. dict or iterable), or we're too deep in the structure to keep walking.
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


@task_revoked.connect
def statsd_task_revoked(sender, request, terminated, signum, expired, **kwargs):
    send_task_completed_event(sender, request.id, sender.backend)