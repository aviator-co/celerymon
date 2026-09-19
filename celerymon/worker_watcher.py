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
    master_maxrss_bytes: dict[str, int]
    master_major_faults: dict[str, int]
    pool_process_count: dict[str, int]
    uptime_seconds: dict[str, int]

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
        self.master_maxrss_bytes = dict()
        self.master_major_faults = dict()
        self.pool_process_count = dict()
        self.uptime_seconds = dict()

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

        master_maxrss_bytes: dict[str, int] = dict()
        master_major_faults: dict[str, int] = dict()
        pool_process_count: dict[str, int] = dict()
        uptime_seconds: dict[str, int] = dict()
        for hostname, stats in (self._inspect.stats() or {}).items():
            # rusage is the string "N/A" where the platform has no resource
            # module, and ru_maxrss is kilobytes on Linux, bytes on macOS.
            rusage = stats.get("rusage")
            if isinstance(rusage, dict):
                master_maxrss_bytes[hostname] = rusage["maxrss"] * 1024
                master_major_faults[hostname] = rusage["majflt"]
            processes = (stats.get("pool") or {}).get("processes")
            if processes is not None:
                pool_process_count[hostname] = len(processes)
            uptime = stats.get("uptime")
            if uptime is not None:
                uptime_seconds[hostname] = uptime

        self.last_updated_timestamp = datetime.datetime.now(tz=datetime.UTC)
        self.oldest_started_task_timestamp = oldest_timestamp
        self.task_count = task_count
        self.master_maxrss_bytes = master_maxrss_bytes
        self.master_major_faults = master_major_faults
        self.pool_process_count = pool_process_count
        self.uptime_seconds = uptime_seconds
