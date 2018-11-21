import inspect
from types import MethodType
from abc import abstractmethod
from celery.app.task import Task

from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_convertion import ConverterRegister


class FireXTask(Task):
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
        BagOfGoodies before running the task
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
        if self._in_required is None:
            self._in_required, self._in_optional = parse_signature(self)
        return list(self._in_required)

    @property
    def optional_args(self) -> list:
        if self._in_required is None:
            self._in_required, self._in_optional = parse_signature(self)
        return list(self._in_optional)


def undecorate(task):
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


def parse_signature(task: Task)->():
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
    while fun:
        try:
            return getattr(fun, attr_name)
        except AttributeError:
            fun = getattr(fun, '__wrapped__', None)
    if default_value:
        return default_value[0]
    raise AttributeError(attr_name)
