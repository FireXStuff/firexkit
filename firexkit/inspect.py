import time
from typing import Iterable

from celery import current_app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


class InspectionReturnedNone(Exception):
    pass


def inspect_with_retry(inspect_retry_timeout=30, inspect_method=None, retry_if_None_returned=True,
                       celery_app=current_app, method_args: Iterable = None, **inspect_opts):
    if method_args is None:
        method_args = ()

    def _inspect():
        i = celery_app.control.inspect(**inspect_opts)
        if inspect_method:
            return getattr(i, inspect_method)(*method_args)
        else:
            return i

    if inspect_retry_timeout:
        timeout_time = time.time() + inspect_retry_timeout
        while time.time() < timeout_time:
            try:
                inspection_result = _inspect()
                if inspection_result is None and retry_if_None_returned:
                    # Inspection might return None if broker didn't respond quickly
                    raise InspectionReturnedNone()
                return inspection_result
            except Exception as e:
                logger.debug(e, exc_info=True)
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


def get_task(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='query_task', **kwargs)
