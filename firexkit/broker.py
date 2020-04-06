import time
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def handle_broker_timeout(callable_func, args=(), kwargs={}, timeout=None, retry_delay=0.1):
    timeout_time = time.time() + timeout if timeout else None
    while True:
        try:
            return callable_func(*args, **kwargs)
        except Exception as e:
            # need to handle different timeout exceptions from different brokers
            if type(e).__name__ != "TimeoutError":
                raise
            if not timeout:
                logger.warning(f'Backend was not reachable and timed out...'
                               f'retrying in {retry_delay}s')
                time.sleep(retry_delay)
            else:
                if time.time() < timeout_time:
                    logger.warning(f'Backend was not reachable and timed out...'
                                   f'retrying in {retry_delay}s for a max of {timeout}s')
                    time.sleep(retry_delay)
                else:
                    logger.error(f'Reached max timeout of {timeout}...giving up!')
                raise