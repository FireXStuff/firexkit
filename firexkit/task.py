import sys

import logging
import os
from collections import OrderedDict
import inspect
import traceback
from celery.result import AsyncResult
from contextlib import contextmanager
from enum import Enum
from logging.handlers import WatchedFileHandler
from types import MethodType
from abc import abstractmethod
from celery.app.task import Task
from celery.local import PromiseProxy
from celery.utils.log import get_task_logger, get_logger

from firexkit.revoke import revoke_recursively
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_conversion import ConverterRegister
from firexkit.result import wait_on_async_results, get_tasks_names_from_results, wait_for_any_results, RETURN_KEYS_KEY, \
    wait_on_async_result_and_maybe_raise, get_result_logging_name

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
    DYNAMIC_RETURN = '__dict__'
    RETURN_KEYS_KEY = RETURN_KEYS_KEY

    def __init__(self):
        self.undecorated = undecorate(self)
        self.sig = inspect.signature(self.run)
        self._decorated_return_keys = getattr(self.undecorated, "_decorated_return_keys", tuple())
        self._task_return_keys = self.get_task_return_keys()
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

    @contextmanager
    def task_context(self):
        try:
            self.context = TaskContext()
            self.initialize_context()
            yield
        finally:
            del self.context

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

    @abstractmethod
    def pre_task_run(self):
        """
        Overrideable method to allow subclasses to do something with the
        BagOfGoodies before returning the results
        """
        pass

    @abstractmethod
    def post_task_run(self, results):
        """
        Overrideable method to allow subclasses to do something with the
        BagOfGoodies after the task has been run
        """
        pass

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
            logger.debug(traceback.format_exc())
            logger.error(e)
            raise
        finally:
            try:
                if self._lagging_children_strategy is not PendingChildStrategy.Continue:
                    self.revoke_pending_children()
            finally:
                self.remove_task_logfile_handler()

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

    def wait_for_specific_children(self, child_results, **kwargs):
        """Wait for the explicitly provided child_results to run and complete"""
        if child_results:
            logger.debug('Waiting for enqueued children: %r' % get_tasks_names_from_results(child_results))
            wait_on_async_results(child_results, caller_task=self, **kwargs)
            [self._update_child_state(child_result, self._UNBLOCKED) for child_result in child_results]

    def enqueue_child(self, chain, add_to_enqueued_children=True, block=False, raise_exception_on_failure=True):
        """Schedule a child task to run"""
        from firexkit.chain import InjectArgs, verify_chain_arguments

        if isinstance(chain, InjectArgs):
            return

        verify_chain_arguments(chain)
        child_result = chain.delay()
        if add_to_enqueued_children:
            self._update_child_state(child_result, self._PENDING)
        if block:
            try:
                wait_on_async_result_and_maybe_raise(result=child_result,
                                                     raise_exception_on_failure=raise_exception_on_failure,
                                                     caller_task=self)
            finally:
                if add_to_enqueued_children:
                    self._update_child_state(child_result, self._UNBLOCKED)
        return child_result

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
    def file_logging_dirpath(self):
        if self._file_logging_dir_path:
            return self._file_logging_dir_path
        else:
            self._file_logging_dir_path = os.path.dirname(self.root_logger_file_handler.baseFilename)
            return self._file_logging_dir_path

    @property
    def task_logging_dirpath(self):
        if self._task_logging_dirpath:
            return self._task_logging_dirpath
        else:
            _task_logging_dirpath = os.path.join(self.file_logging_dirpath, self.request.hostname)
            if not os.path.exists(_task_logging_dirpath):
                os.makedirs(_task_logging_dirpath, mode=0o777, exist_ok=True)
            self._task_logging_dirpath = _task_logging_dirpath
            return self._task_logging_dirpath

    @property
    def task_logfile(self):
        filename = '%s_%s.html' % (self.name, str(self.request.id))
        return os.path.join(self.task_logging_dirpath, filename)

    def add_task_logfile_handler(self):
        task_logfile = self.task_logfile
        self._temp_loghandlers = {}
        fh_root = logging.handlers.WatchedFileHandler(task_logfile, mode='a+')
        fh_root.setLevel(logging.DEBUG)
        fh_root.setFormatter(self.root_logger_file_handler.formatter)
        self.root_logger.addHandler(fh_root)
        self._temp_loghandlers[self.root_logger] = fh_root

        task_logger = get_logger('celery.task')
        fh_task = logging.FileHandler(task_logfile, mode='a+')
        fh_task.setLevel(logging.DEBUG)
        original_file_handler = [handler for handler in task_logger.handlers if
                                 isinstance(handler, WatchedFileHandler)][0]
        fh_task.setFormatter(original_file_handler.formatter)
        task_logger.addHandler(fh_task)
        self._temp_loghandlers[task_logger] = fh_task

    def remove_task_logfile_handler(self):
        if self._temp_loghandlers:
            for _logger, _handler in self._temp_loghandlers.items():
                _logger.removeHandler(_handler)


def undecorate(task):
    """:return: the original function that was used to create a microservice"""
    undecorated_func = task.run
    while True:
        try:
            undecorated_func = getattr(undecorated_func, '__wrapped__')
        except AttributeError:
            break
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
