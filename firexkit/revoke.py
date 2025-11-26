import datetime
import typing

from celery import current_app
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

    def __init__(self):
        self.timer_expiry = datetime.timedelta(seconds=60)
        self.revoked_uuids: set[str] = set()
        self.last_updated = datetime.datetime.now(tz=datetime.timezone.utc)

        from firexapp.engine.firex_celery import FireXCeleryInspector
        self.inspector = FireXCeleryInspector(current_app)

    def update(self):
        # from firexapp.engine.firex_celery import FireXCelery
        # current_app : FireXCelery
        revoked_tasks = self.inspector.get_revoked_tasks(
            timeout=60,
            worker_ids=[current_app.get_mc_worker_id()],
        )
        self.revoked_uuids.update(
            {t.id for t in revoked_tasks if t.id},
        )
        self.last_updated = datetime.datetime.now(tz=datetime.timezone.utc)

    def is_revoked(self, result_id: typing.Optional[str]) -> bool:
        if result_id:
            if result_id in self.revoked_uuids:
                return True
            else:
                # Updating the revoked_list is an expensive operation, so only do it periodically
                time_lapsed = datetime.datetime.now(tz=datetime.timezone.utc) - self.last_updated
                if time_lapsed > self.timer_expiry:
                    self.update()
                    return result_id in self.revoked_uuids

        return False
