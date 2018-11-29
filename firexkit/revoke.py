from celery import current_app
from datetime import timedelta, datetime
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


class RevokedRequests(object):
    """
     Need to inspect the app for the revoked requests, because AsyncResult.state of a task that hasn't
    been de-queued and executed by a worker but was revoked is PENDING (i.e., the REVOKED state is only updated upon
    executing a task). This phenomenon makes the wait_for_results wait on such "revoked" tasks, and therefore
    required us to implement this work-around.
    """

    _instance = None

    @classmethod
    def instance(cls, existing_instance=None):
        if existing_instance is not None:
                cls._instance = existing_instance
        if cls._instance is None:
            cls._instance = RevokedRequests()
        return cls._instance

    def __init__(self, timer_expiry_secs=60):
        self.timer_expiry = timedelta(seconds=timer_expiry_secs)
        self.revoked_list = None
        self.last_updated = None

    @classmethod
    def get_revoked_list_from_app(cls):
        revoked_list = list()
        v = current_app.control.inspect().revoked()
        if not v:
            return revoked_list
        else:
            v = v.values()
        if v:
            for l in v:
                revoked_list += l
        return revoked_list

    def update(self):
        self.revoked_list = self.get_revoked_list_from_app()
        self.last_updated = datetime.utcnow()
        logger.debug('RevokedRequests list updated at %s to %r' % (self.last_updated, self.revoked_list))

    def _task_in_revoked_list(self, result_id):
        if self.last_updated is None:
            self.update()
        return result_id in self.revoked_list

    def is_revoked(self, result_id, timer_expiry_secs=None):
        if self._task_in_revoked_list(result_id):
            return True
        else:
            # Updating the revoked_list is an expensive operation, so only do it periodically
            timer_expiry = self.timer_expiry if timer_expiry_secs is None else \
                timedelta(seconds=timer_expiry_secs)
            time_lapsed = datetime.utcnow()-self.last_updated
            if time_lapsed > timer_expiry:
                logger.debug('%s since last update of RevokedRequests list; updating now' % time_lapsed)
                self.update()
                return self._task_in_revoked_list(result_id)
            else:
                return False
