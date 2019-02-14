import inspect
from enum import Enum
from types import MethodType
from abc import abstractmethod
from celery.app.task import Task
from celery.local import PromiseProxy
from celery.utils.log import get_task_logger

from firexkit.revoke import revoke_recursively
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_conversion import ConverterRegister
from firexkit.result import wait_on_async_results, get_tasks_names_from_results, wait_for_any_results

logger = get_task_logger(__name__)


class PendingChildStrategy(Enum):
    """
    Available strategies for handling remaining pending child tasks upon successful completion
    of the parent microservice.
    """

    Block = 0, "Default"
    Revoke = 1
    Continue = 2


class FireXTask(Task):
    """
    Task object that facilitates passing of arguments and return values from one task to another, to be used in chains
    """

    def __init__(self):
        self.undecorated = undecorate(self)
        self.return_keys = getattr(self.undecorated, "_return_keys", tuple())
        self._lagging_children_strategy = get_attr_unwrapped(self, 'pending_child_strategy', PendingChildStrategy.Block)

        super(FireXTask, self).__init__()

        self._in_required = None
        self._in_optional = None

        self._enqueued_children = {}

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError('Tasks must define the run method.')

    @abstractmethod
    def pre_task_run(self, bag_of_goodies: BagOfGoodies):
        """
        Overrideable method to allow subclasses to do something with the
        BagOfGoodies before returning the results
        """
        pass

    @abstractmethod
    def post_task_run(self, results, bag_of_goodies: BagOfGoodies):
        """
        Overrideable method to allow subclasses to do something with the
        BagOfGoodies after the task has been run
        """
        pass

    def __call__(self, *args, **kwargs):
        try:
            result = self._process_arguments_and_run(*args, **kwargs)

            if self._lagging_children_strategy is PendingChildStrategy.Block:
                try:
                    self.wait_for_children()
                except Exception as e:
                    logger.debug("The following exception was thrown (and caught) when wait_for_children was "
                                 "implicitly called by this task's base class:\n" + str(e))
            return result
        finally:
            if self._lagging_children_strategy is not PendingChildStrategy.Continue:
                self.revoke_pending_children()

    def _process_arguments_and_run(self, *args, **kwargs):
        # Organise the input args by creating a BagOfGoodies
        sig = inspect.signature(self.run)
        bog = BagOfGoodies(sig, args, kwargs)

        # run any "pre" converters attached to this task
        converted = ConverterRegister.task_convert(task_name=self.name, pre_task=True, **bog.get_bag())
        bog.update(converted)

        # give sub-classes a change to do something with the args
        self.pre_task_run(bog)

        args, kwargs = bog.split_for_signature()
        result = super(FireXTask, self).__call__(*args, **kwargs)

        # Need to update the dict with the results, if @results was used
        if isinstance(result, dict):
            bog.update(result)

        # run any post converters attached to this task
        converted = ConverterRegister.task_convert(task_name=self.name, pre_task=False, **bog.get_bag())
        bog.update(converted)

        if isinstance(result, dict):
            # update the results with changes from converters
            result = {k: v for k, v in bog.get_bag().items() if k in result}

        # give sub-classes a change to do something with the results
        self.post_task_run(result, bog)

        return bog.get_bag()

    @property
    def required_args(self) -> list:
        """
        :return: list of required arguments to the microservice.
        """
        if self._in_required is None:
            self._in_required, self._in_optional = parse_signature(self)
        return list(self._in_required)

    @property
    def optional_args(self) -> dict:
        """
        :return: dict of optional arguments to the microservice, and their values.
        """
        if self._in_required is None:
            self._in_required, self._in_optional = parse_signature(self)
        return dict(self._in_optional)

    #######################
    # Enqueuing child tasks

    _STATE_KEY = 'state'
    _PENDING = 'pending'
    _UNBLOCKED = 'unblocked'

    @property
    def enqueued_children(self):
        return list(self._enqueued_children.keys())

    @property
    def pending_enqueued_children(self):
        return [child for child, result in self._enqueued_children.items() if
                result.get(self._STATE_KEY) == self._PENDING]

    def _add_enqueued_child(self, child_result):
        if child_result not in self._enqueued_children:
            self._enqueued_children[child_result] = {}

    def _update_child_state(self, child_result, state):
        if child_result not in self._enqueued_children:
            self._add_enqueued_child(child_result)
        self._enqueued_children[child_result][self._STATE_KEY] = state

    def wait_for_children(self, pending_only=True, **kwargs):
        """Wait for all enqueued child tasks to run and complete"""
        child_results = self.pending_enqueued_children if pending_only else self.enqueued_children
        if child_results:
            logger.debug('Waiting for enqueued children: %r' % get_tasks_names_from_results(child_results))
            wait_on_async_results(child_results, caller_task=self, **kwargs)
            [self._update_child_state(child_result, self._UNBLOCKED) for child_result in child_results]

    def wait_for_any_children(self, pending_only=True, **kwargs):
        child_results = self.pending_enqueued_children if pending_only else self.enqueued_children
        for completed_child_result in wait_for_any_results(child_results, **kwargs):
            self._update_child_state(completed_child_result, self._UNBLOCKED)
            yield completed_child_result

    def enqueue_child(self, chain, add_to_enqueued_children=True, **kwargs):
        """Schedule a child task to run"""
        from firexkit.chain import InjectArgs
        if isinstance(chain, InjectArgs):
            return

        child_result = chain.enqueue(caller_task=self, **kwargs)
        if add_to_enqueued_children:
            self._add_enqueued_child(child_result)
            if not kwargs.get('block', False):
                self._update_child_state(child_result, self._PENDING)
        return child_result

    def revoke_pending_children(self):
        pending_children = self.pending_enqueued_children
        if pending_children:
            logger.info('Pending children of current task exist. '
                        'Revoking %r' % get_tasks_names_from_results(pending_children))
            revoke_recursively(pending_children)


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


def parse_signature(task: Task)->(set, dict):
    """Parse the run function of a microservice and return required and optional arguments"""
    required = set()
    optional = {}
    for k, v in inspect.signature(task.run).parameters.items():
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
