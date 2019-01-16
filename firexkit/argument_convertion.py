
from collections import namedtuple
from datetime import datetime
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy

from firexkit.bag_of_goodies import BagOfGoodies


logger = get_task_logger(__name__)


class ConverterRegister:
    _ConvertNode = namedtuple('ConvertNode', ['func', 'dependencies'])
    _task_instances = {}

    def __init__(self):
        """A register for argument converter functions that take in kwargs and transforms them."""
        self._pre_converters = {}
        self._post_converters = {}
        self.visit_order = None

    @classmethod
    def task_convert(cls, task_name: str, pre_task=True, **kwargs) -> dict:
        """
        Run the argument conversion for a given task.

        :param task_name: the short name of the task. If long name is given, it will be reduced to that short name
        :param pre_task: Converters can be registered to run before or after a task runs
        :param kwargs: the argument dict to be converted
        """
        task_short_name = task_name.split('.')[-1]
        if task_short_name not in cls._task_instances:
            return kwargs
        task = cls._task_instances[task_short_name]

        return task.convert(pre_task=pre_task, **kwargs)

    def convert(self, pre_task=True, **kwargs) -> dict:
        """ Run all registered converters
         :param pre_task: Converters can be registered to run before or after a task runs.
         """
        # Recreate the kwargs and remove any argument string
        # values which start with '@'.
        new_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, str):
                if not v.startswith(BagOfGoodies.INDIRECT_ARG_CHAR):
                    new_kwargs[k] = v
            else:
                new_kwargs[k] = v

        # we converting pre or post
        converters = self._pre_converters if pre_task else self._post_converters

        for node in self.get_visit_order(pre_task):
            start = datetime.now()
            try:
                converted_dict = converters[node].func(new_kwargs)
            except Exception:
                logger.error("Error in converter " + node)
                raise
            done = datetime.now()
            logger.debug("Took %.3f seconds to convert %s" % ((done - start).total_seconds(), node))
            # handle when None is returned
            if converted_dict:
                new_kwargs.update(converted_dict)
        kwargs.update(new_kwargs)
        return kwargs

    def get_visit_order(self, pre_task=True):
        """Provide a list of all converters in order of execution, accounting for dependencies."""
        converters = self._pre_converters if pre_task else self._post_converters
        self.visit_order = []
        for converter_node in converters.values():
            self._visit_converter(converter_node=converter_node, all_converters=converters)
        return self.visit_order

    def _visit_converter(self, all_converters, converter_node, depth=0):
        if depth > len(all_converters):
            raise CircularDependencyException("A circular dependency was detected between converters")

        if converter_node.func.__name__ in self.visit_order:
            return

        for precursor in converter_node.dependencies:
            if precursor not in all_converters and precursor not in self._pre_converters:
                msg = precursor + " was not found. It is a dependency of " + converter_node.func.__name__
                raise MissingConverterDependencyError(msg)

            if converter_node.func.__name__ == precursor:
                raise CircularDependencyException("A converter can not be dependant on itself")

            if precursor not in self.visit_order and precursor in all_converters:
                self._visit_converter(all_converters=all_converters,
                                      converter_node=all_converters[precursor],
                                      depth=depth+1)
        self.visit_order.append(converter_node.func.__name__)

    @classmethod
    def register_for_task(cls, task: PromiseProxy, *args):
        """ Register a converter function for a given task.

        :param task: A microservice signature against which to register the task
        :param args: A collection of optional arguments, the function of which is based on it's type:

        *  **callable** (only once): A function that will be called to convert arguments
        *  **boolean** (only once): At which point should this converter be called? False is pre (before task), \
                 True is post. (after task)
        *  **str**: Dependencies. Any dependency of the current converter on the one in the string.

        """
        task_short_name = task.name.split('.')[-1]
        if task_short_name not in cls._task_instances:
            cls._task_instances[task_short_name] = ConverterRegister()
        task_registry = cls._task_instances[task_short_name]
        if not len(args):
            raise ConverterRegistrationException("Task Registration requires an additional bool parameter. "
                                                 "False is pre-task")
        return task_registry.register(*args)

    def register(self, *args):
        """ Register a converter function.

        :param args: A collection of optional arguments, the function of which is based on it's type:

        * **callable** (only once): A function that will be called to convert arguments
        * **boolean** (only once): At which point should this converter be called? False is pre (before task), \
                True is post. (after task)
        * **str**: Dependencies. Any dependency of the current converter on the one in the string.

        """

        if len(args) is 0:
            raise ConverterRegistrationException("Registration requires at least one argument")

        func = None
        dependencies = []
        run_post_task = None

        for arg in args:
            if callable(arg):
                func = arg
            elif isinstance(arg, str):
                dependencies.append(arg)
            elif isinstance(arg, bool):
                run_post_task = arg
            else:
                raise ConverterRegistrationException(
                    "Converter incorrectly registered. Type %s not recognised" % str(type(arg)))

        return self._sub_register(func=func, dependencies=dependencies, run_post_task=run_post_task)

    def _sub_register(self, func, dependencies: [], run_post_task):

        if not run_post_task:
            converters = self._pre_converters
        else:
            converters = self._post_converters

        if func:
            # this is the case where decorator is used WITHOUT parenthesis
            # @InputConverter.register
            self._check_not_registered(func)
            converters[func.__name__] = self._ConvertNode(func=func, dependencies=[])
            return func

        # this is the case where decorator is used WITH parenthesis
        # @ConverterRegister.register(...)
        def _wrapped_register(fn):
            self._check_not_registered(fn)
            converters[fn.__name__] = self._ConvertNode(func=fn, dependencies=dependencies)
            return fn
        return _wrapped_register

    def _check_not_registered(self, func):
        if callable(func):
            func = func.__name__

        if func in self._pre_converters or func in self._post_converters:
            raise ConverterRegistrationException("Converter %s is already registered. "
                                                 "Please define a unique name" % func)

    @classmethod
    def get_register(cls, task_name):
        task_short_name = task_name.split('.')[-1]
        return cls._task_instances.get(task_short_name)


class MissingConverterDependencyError(Exception):
    """A converter was registered with a dependency that does not exist."""
    pass


class CircularDependencyException(Exception):
    """A converter was registered with a dependency that is itself directly or indirectly dependent on it."""
    pass


class ConverterRegistrationException(Exception):
    pass
