import inspect
from abc import abstractmethod
from celery.app.task import Task

from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.argument_convertion import ConverterRegister


class FireXTask(Task):
    def __init__(self):
        super(FireXTask, self).__init__()

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
