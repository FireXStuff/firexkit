import time
from typing import Iterable

from celery import current_app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def inspect_with_retry(inspect_retry_timeout=30, inspect_method=None, retry_if_None_returned=True,
                       celery_app=current_app, method_args: Iterable = None, verbose=False, **inspect_opts):

    inspect_retry_timeout = inspect_retry_timeout if inspect_retry_timeout else 0
    timeout_time = time.time() + inspect_retry_timeout

    if method_args is None:
        method_args = ()

    def _get_result_summary(result):
        if result:
            return {k: len(v) for k,v in result.items()}
        else:
            return result

    def _inspect(celery_app, inspect_method, method_args, **inspect_opts):
        i = celery_app.control.inspect(**inspect_opts)
        if inspect_method:
            header = f'[inspect] app.control.inspect({inspect_opts or ""}).{inspect_method}({method_args or ""})'
            if verbose:
                logger.debug(header)

            result = getattr(i, inspect_method)(*method_args)

            if verbose:
                logger.debug(f'{header} returned {_get_result_summary(result)}')

            return result
        else:
            header = f'[inspect] app.control.inspect({inspect_opts or ""})'
            if verbose:
                logger.debug(header)

            return i

    inspection_result = _inspect(celery_app, inspect_method, method_args, **inspect_opts)
    while inspection_result is None and retry_if_None_returned and time.time() < timeout_time:
        time.sleep(0.1)
        logger.debug(f'[inspect] Retrying for a maximum of {inspect_retry_timeout}s')
        inspection_result = _inspect(celery_app, inspect_method, method_args, **inspect_opts)
    return inspection_result


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
