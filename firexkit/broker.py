import time
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def handle_broker_timeout(callable_func, args=(), kwargs={}, timeout=None, retry_delay=0.1):
    timeout_time = time.time() + timeout if timeout else None
    retries = 0
    while True:
        try:
            return_value = callable_func(*args, **kwargs)
        except Exception as e:
            # need to handle different timeout exceptions from different brokers
            if type(e).__name__ != "TimeoutError":
                raise
            if not timeout:
                logger.warning(f'Backend was not reachable and timed out...'
                               f'retrying in {retry_delay}s')
                time.sleep(retry_delay)
                retries += 1
            else:
                current_time = time.time()
                if current_time < timeout_time:
                    logger.warning(f'Backend was not reachable and timed out...'
                                   f'retrying in {retry_delay}s for a max of {timeout}s ({timeout_time-current_time:.2f}s remaining)')
                    time.sleep(retry_delay)
                    retries += 1
                else:
                    logger.error(f'Reached max timeout of {timeout}...giving up!')
                    try:
                        instrument_and_send_event('handle_broker_timeout-failure', broker_retries=retries)
                    finally:
                        raise
        else:
            try:
                instrument_and_send_event('handle_broker_timeout-success', broker_retries=retries)
            finally:
                return return_value


def instrument_and_send_event(instrumentation_label, **kwargs):
    from celery import current_task
    if not current_task:
        return
    try:
        current_task.send_event('task-instrumentation', instrumentation_label=instrumentation_label, **kwargs)
    except Exception as e:
        logger.debug(e, exc_info=True)
        logger.debug('Could not instrument')
