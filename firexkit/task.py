import inspect
from types import MethodType
from abc import abstractmethod
from celery.app.task import Task

from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_conversion import ConverterRegister


class FireXTask(Task):
    """
    Task object that facilitates passing of arguments and return values from one task to another, to be used in chains
    """
    def __init__(self):
        self.undecorated = undecorate(self)
        self.return_keys = getattr(self.undecorated, "_return_keys", tuple())
        super(FireXTask, self).__init__()

        self._in_required = None
        self._in_optional = None

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
