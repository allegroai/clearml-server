from datetime import timedelta, datetime
from time import sleep

from apiserver.bll.task import update_project_time
from apiserver.config_repo import config
from apiserver.database.model.task.task import TaskStatus, Task
from apiserver.utilities.threads_manager import ThreadsManager

log = config.logger(__file__)


class NonResponsiveTasksWatchdog:
    threads = ThreadsManager()

    class _Settings:
        """
        Retrieves watchdog settings from the config file
        The properties are not cached so that the updates in
        the config file are reflected
        """

        _prefix = "services.tasks.non_responsive_tasks_watchdog"

        @property
        def enabled(self):
            return config.get(f"{self._prefix}.enabled", True)

        @property
        def watch_interval_sec(self):
            return config.get(f"{self._prefix}.watch_interval_sec", 900)

        @property
        def threshold_sec(self):
            return config.get(f"{self._prefix}.threshold_sec", 7200)

    settings = _Settings()

    @classmethod
    @threads.register("non_responsive_tasks_watchdog", daemon=True)
    def start(cls):
        sleep(cls.settings.watch_interval_sec)
        while True:
            watch_interval = cls.settings.watch_interval_sec
            if cls.settings.enabled:
                try:
                    stopped = cls.cleanup_tasks(
                        threshold_sec=cls.settings.threshold_sec
                    )
                    log.info(f"{stopped} non-responsive tasks stopped")
                except Exception as ex:
                    log.exception(f"Failed stopping tasks: {str(ex)}")
            sleep(watch_interval)

    @classmethod
    def cleanup_tasks(cls, threshold_sec):
        relevant_status = (TaskStatus.in_progress,)
        threshold = timedelta(seconds=threshold_sec)
        ref_time = datetime.utcnow() - threshold
        log.info(
            f"Starting cleanup cycle for running tasks last updated before {ref_time}"
        )

        tasks = list(
            Task.objects(status__in=relevant_status, last_update__lt=ref_time).only(
                "id", "name", "status", "project", "last_update"
            )
        )
        log.info(f"{len(tasks)} non-responsive tasks found")
        if not tasks:
            return 0

        err_count = 0
        project_ids = set()
        now = datetime.utcnow()
        for task in tasks:
            log.info(
                f"Stopping {task.id} ({task.name}), last updated at {task.last_update}"
            )
            # noinspection PyBroadException
            try:
                updated = Task.objects(id=task.id, status=task.status).update(
                    status=TaskStatus.stopped,
                    status_reason="Forced stop (non-responsive)",
                    status_message="Forced stop (non-responsive)",
                    status_changed=now,
                    last_update=now,
                    last_change=now,
                )
                if updated:
                    project_ids.add(task.project)
                else:
                    err_count += 1
            except Exception as ex:
                log.error("Failed setting status: %s", str(ex))

        update_project_time(list(project_ids))

        return len(tasks) - err_count
