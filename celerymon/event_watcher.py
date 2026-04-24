# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

import datetime
import logging
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, NamedTuple, Sequence

import celery

from .timer import RepeatTimer

logger = logging.getLogger(__name__)

_WORKER_HEARTBEAT_TTL_SEC = 120
_PRUNE_INTERVAL_SEC = 30


def _normalize_buckets(buckets: Sequence[float | str]) -> list[float]:
    bounds = [float(b) for b in buckets]
    if bounds and bounds[-1] != float("inf"):
        bounds.append(float("inf"))
    bounds.sort()
    return bounds


def _parse_expires(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value).timestamp()
        except ValueError:
            return None
    return None


class InFlightEntry(NamedTuple):
    sent_ts: float
    task_name: str
    queue_name: str | None
    expires_ts: float | None


class EventWatcher:
    _TASK_NAMES_CACHE_LIMIT = 100_000

    last_received_timestamp: datetime.datetime | None
    last_received_timestamp_per_task_event: dict[tuple[str, str], datetime.datetime]
    num_events_per_task_count: dict[tuple[str, str], int]
    upper_bounds: list[float]
    task_names: set[str]
    task_runtime_sec: list[dict[tuple[str, str], int]]
    task_runtime_sec_sum: dict[tuple[str, str], float]
    queue_wait_upper_bounds: list[float]
    queue_wait_sec: list[dict[str, int]]
    queue_wait_sec_sum: dict[str, float]

    @classmethod
    def create_started(
        cls,
        app: celery.Celery,
        buckets: Sequence[float | str],
        queue_wait_buckets: Sequence[float | str],
        in_flight_cache_size: int,
        in_flight_ttl_sec: int,
    ):
        store = cls(
            buckets, queue_wait_buckets, in_flight_cache_size, in_flight_ttl_sec
        )

        def run() -> None:
            backoff = 1.0
            max_backoff = 60.0
            while True:
                try:
                    with app.connection() as conn:
                        recv = app.events.Receiver(conn, handlers={"*": store.on_event})
                        logger.info("EventWatcher connected, capturing events")
                        backoff = 1.0
                        recv.capture(limit=None)
                except Exception:
                    logger.exception(
                        "EventWatcher connection lost, reconnecting in %.1fs",
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        def update_enable_event() -> None:
            try:
                app.control.enable_events()
            except Exception:
                logger.exception("Failed to enable events")

        def prune() -> None:
            try:
                store._prune(datetime.datetime.now(tz=datetime.UTC))
            except Exception:
                logger.exception("EventWatcher prune failed")

        timer = RepeatTimer(10, update_enable_event)
        timer.daemon = True
        timer.start()

        prune_timer = RepeatTimer(_PRUNE_INTERVAL_SEC, prune)
        prune_timer.daemon = True
        prune_timer.start()

        thread = threading.Thread(
            target=run, daemon=True, name="celerymon-event-watcher"
        )
        thread.start()

        return store

    def __init__(
        self,
        buckets: Sequence[float | str],
        queue_wait_buckets: Sequence[float | str],
        in_flight_cache_size: int,
        in_flight_ttl_sec: int,
    ):
        self._task_names_by_uuid: OrderedDict[str, str] = OrderedDict()
        self._worker_last_heartbeat: dict[str, datetime.datetime] = dict()
        self._in_flight: OrderedDict[str, InFlightEntry] = OrderedDict()
        self._in_flight_cache_size = in_flight_cache_size
        self._in_flight_ttl_sec = in_flight_ttl_sec
        self._eviction_counts: dict[str, int] = defaultdict(int)

        self.upper_bounds = _normalize_buckets(buckets)
        self.queue_wait_upper_bounds = _normalize_buckets(queue_wait_buckets)
        self.task_names = set()

        self.last_received_timestamp = None
        self.last_received_timestamp_per_task_event = dict()
        self.num_events_per_task_count = defaultdict(int)
        self.task_runtime_sec = [
            defaultdict(int) for _ in range(len(self.upper_bounds))
        ]
        self.task_runtime_sec_sum = defaultdict(float)
        self.queue_wait_sec = [
            defaultdict(int) for _ in range(len(self.queue_wait_upper_bounds))
        ]
        self.queue_wait_sec_sum = defaultdict(float)

    def on_event(self, event: dict[str, Any]):
        now = datetime.datetime.now(tz=datetime.UTC)
        self.last_received_timestamp = now

        event_name: str = event["type"]

        if event_name.startswith("worker-"):
            self._on_worker_event(event_name, event, now)
            return

        if not event_name.startswith("task-"):
            return

        uuid: str = event["uuid"]
        if "name" in event:
            self._task_names_by_uuid.pop(uuid, None)
            self._task_names_by_uuid[uuid] = event["name"]
            while len(self._task_names_by_uuid) > self._TASK_NAMES_CACHE_LIMIT:
                self._task_names_by_uuid.popitem(last=False)

        task_name = self._task_names_by_uuid.get(uuid, "(UNKNOWN)")
        self.task_names.add(task_name)

        self.last_received_timestamp_per_task_event[(task_name, event_name)] = now
        self.num_events_per_task_count[(task_name, event_name)] += 1

        if event_name == "task-sent":
            self._record_task_sent(uuid, task_name, event)

        if event_name == "task-started":
            self._record_task_started(uuid, task_name, event)

        if event_name == "task-revoked":
            if self._in_flight.pop(uuid, None) is not None:
                self._eviction_counts["revoked_pre_start"] += 1

        if event_name == "task-succeeded":
            self._record_task_runtime(task_name, "success", event)

        if event_name == "task-failed":
            if self._in_flight.pop(uuid, None) is not None:
                self._eviction_counts["failed_pre_start"] += 1
            self._record_task_runtime(task_name, "failed", event)

    def _on_worker_event(
        self,
        event_name: str,
        event: dict[str, Any],
        now: datetime.datetime,
    ) -> None:
        hostname = event.get("hostname")
        if not hostname:
            return
        if event_name == "worker-offline":
            self._worker_last_heartbeat.pop(hostname, None)
        else:
            self._worker_last_heartbeat[hostname] = now

    def online_worker_count(
        self,
        now: datetime.datetime,
        ttl_sec: int = _WORKER_HEARTBEAT_TTL_SEC,
    ) -> int:
        cutoff = now - datetime.timedelta(seconds=ttl_sec)
        heartbeats = tuple(self._worker_last_heartbeat.values())
        return sum(1 for ts in heartbeats if ts > cutoff)

    def _record_task_runtime(
        self,
        task_name: str,
        result: str,
        event: dict[str, Any],
    ) -> None:
        # Celery sets `runtime` (seconds) on task-succeeded consistently, and on
        # task-failed when the task actually ran. Defensive `.get()` handles
        # failure modes where the task never executed.
        runtime_sec = event.get("runtime")
        if runtime_sec is None:
            logger.debug(
                "task event missing runtime field; task_name=%s result=%s",
                task_name,
                result,
            )
            return
        key = (task_name, result)
        self.task_runtime_sec_sum[key] += runtime_sec
        for i, bound in enumerate(self.upper_bounds):
            if runtime_sec <= bound:
                self.task_runtime_sec[i][key] += 1

    def _record_task_sent(
        self,
        uuid: str,
        task_name: str,
        event: dict[str, Any],
    ) -> None:
        sent_ts = event.get("timestamp")
        if sent_ts is None:
            return
        self._in_flight.pop(uuid, None)
        self._in_flight[uuid] = InFlightEntry(
            sent_ts=float(sent_ts),
            task_name=task_name,
            queue_name=event.get("queue"),
            expires_ts=_parse_expires(event.get("expires")),
        )
        while len(self._in_flight) > self._in_flight_cache_size:
            self._in_flight.popitem(last=False)
            self._eviction_counts["lru"] += 1

    def _record_task_started(
        self,
        uuid: str,
        task_name: str,
        event: dict[str, Any],
    ) -> None:
        entry = self._in_flight.pop(uuid, None)
        if entry is None:
            return
        started_ts = event.get("timestamp")
        if started_ts is None:
            return
        wait_sec = float(started_ts) - entry.sent_ts
        if wait_sec < 0:
            return
        self.queue_wait_sec_sum[task_name] += wait_sec
        for i, bound in enumerate(self.queue_wait_upper_bounds):
            if wait_sec <= bound:
                self.queue_wait_sec[i][task_name] += 1

    def oldest_queued_age_by_queue(
        self,
        now: datetime.datetime,
    ) -> dict[str, float]:
        """Pure read: walks the in-flight cache, returns per-queue max age of
        entries that are still eligible (not expired, not past TTL). Does not
        mutate state; eviction happens on the prune timer.
        """
        now_ts = now.timestamp()
        ttl_cutoff = now_ts - self._in_flight_ttl_sec
        ages: dict[str, float] = {}
        for entry in self._in_flight.values():
            if entry.expires_ts is not None and entry.expires_ts < now_ts:
                continue
            if entry.sent_ts < ttl_cutoff:
                continue
            if entry.queue_name is None:
                continue
            age = now_ts - entry.sent_ts
            current = ages.get(entry.queue_name)
            if current is None or age > current:
                ages[entry.queue_name] = age
        return ages

    def _prune(self, now: datetime.datetime) -> None:
        """Evict expired / past-TTL in-flight entries and stale worker
        heartbeats. Runs on its own timer so that metric collection stays a
        pure read.
        """
        now_ts = now.timestamp()
        ttl_cutoff = now_ts - self._in_flight_ttl_sec
        to_evict: list[tuple[str, str]] = []
        for uuid, entry in list(self._in_flight.items()):
            if entry.expires_ts is not None and entry.expires_ts < now_ts:
                to_evict.append((uuid, "expired"))
            elif entry.sent_ts < ttl_cutoff:
                to_evict.append((uuid, "ttl"))
        for uuid, reason in to_evict:
            if self._in_flight.pop(uuid, None) is not None:
                self._eviction_counts[reason] += 1

        heartbeat_cutoff = now - datetime.timedelta(seconds=_WORKER_HEARTBEAT_TTL_SEC)
        for hostname, ts in list(self._worker_last_heartbeat.items()):
            if ts <= heartbeat_cutoff:
                self._worker_last_heartbeat.pop(hostname, None)

    def eviction_counts(self) -> dict[str, int]:
        return dict(self._eviction_counts)

    def in_flight_cache_size(self) -> int:
        return len(self._in_flight)
