import time
from celery import current_app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def inspect_with_retry(inspect_retry_timeout=30, inspect_method=None, **inspect_opts):
    def _inspect():
        i = current_app.control.inspect(**inspect_opts)
        if inspect_method:
            return getattr(i, inspect_method)()
        else:
            return i

    if inspect_retry_timeout:
        timeout_time = time.time() + inspect_retry_timeout
        while time.time() < timeout_time:
            try:
                return _inspect()
            except Exception as e:
                logger.debug(e)
                logger.debug('Inspection failed. Retrying for up to %r seconds' % inspect_retry_timeout)
                time.sleep(0.1)
    return _inspect()


def get_active(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='active', **kwargs)


def get_reserved(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='reserved', **kwargs)


def get_scheduled(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='scheduled', **kwargs)


def get_revoked(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='revoked', **kwargs)


def get_active_queues(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='active_queues', **kwargs)
