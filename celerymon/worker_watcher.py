# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import datetime
from collections import defaultdict

import celery

from .event_watcher import EventWatcher
from .timer import RepeatTimer


class WorkerWatcher:
    last_updated_timestamp: datetime.datetime | None
    oldest_started_task_timestamp: dict[str, datetime.datetime]
    task_count: dict[tuple[str, str, str], int]

    @classmethod
    def create_started(
        cls,
        app: celery.Celery,
        interval: float,
        event_watcher: EventWatcher,
    ) -> WorkerWatcher:
        watcher = cls(app, event_watcher)

        timer = RepeatTimer(interval, watcher._update)
        timer.start()

        return watcher

    def __init__(self, app: celery.Celery, event_watcher: EventWatcher):
        self._inspect = app.control.inspect()
        self._event_watcher = event_watcher
        self.last_updated_timestamp = None
        self.oldest_started_task_timestamp = dict()
        self.task_count = dict()

    def _update(self) -> None:
        oldest_timestamp: dict[str, datetime.datetime] = dict()
        task_count: dict[tuple[str, str, str], int] = defaultdict(int)

        for hostname, tasks in (self._inspect.active() or {}).items():
            for task in tasks:
                if isinstance(task["time_start"], str):
                    start_time = datetime.datetime.fromisoformat(task["time_start"])
                else:
                    start_time = datetime.datetime.fromtimestamp(task["time_start"])
                task_name = task["type"]
                self._event_watcher.record_task_name(task["id"], task_name)
                task_count[("active", task_name, hostname)] += 1
                if task_name not in oldest_timestamp:
                    oldest_timestamp[task_name] = start_time
                else:
                    oldest_timestamp[task_name] = min(
                        oldest_timestamp[task_name], start_time
                    )
        for hostname, tasks in (self._inspect.reserved() or {}).items():
            for task in tasks:
                task_name = task["type"]
                self._event_watcher.record_task_name(task["id"], task_name)
                task_count[("reserved", task_name, hostname)] += 1
        for hostname, scheduled_tasks in (self._inspect.scheduled() or {}).items():
            for scheduled_task in scheduled_tasks:
                request = scheduled_task["request"]
                task_name = request["type"]
                self._event_watcher.record_task_name(request["id"], task_name)
                task_count[("scheduled", task_name, hostname)] += 1

        self.last_updated_timestamp = datetime.datetime.now(tz=datetime.UTC)
        self.oldest_started_task_timestamp = oldest_timestamp
        self.task_count = task_count
