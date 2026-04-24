# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import datetime
from typing import Iterable

import prometheus_client
from prometheus_client.metrics_core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)
from prometheus_client.utils import floatToGoString

from .event_watcher import EventWatcher
from .redis_watcher import RedisWatcher
from .worker_watcher import WorkerWatcher


class Collector(prometheus_client.registry.Collector):
    def __init__(
        self,
        redis_watcher: RedisWatcher | None,
        worker_watcher: WorkerWatcher | None,
        event_watcher: EventWatcher | None,
    ):
        self._redis_watcher = redis_watcher
        self._worker_watcher = worker_watcher
        self._event_watcher = event_watcher

    def describe(self) -> Iterable[prometheus_client.Metric]:
        return self.collect()

    def collect(self) -> Iterable[prometheus_client.Metric]:
        metrics: list[prometheus_client.Metric] = []
        if self._redis_watcher is not None:
            metrics.extend(redis_metrics(self._redis_watcher))
        if self._worker_watcher is not None:
            metrics.extend(worker_metrics(self._worker_watcher))
        if self._event_watcher is not None:
            metrics.extend(event_metrics(self._event_watcher))
        return metrics


def redis_metrics(watcher: RedisWatcher) -> list[prometheus_client.Metric]:
    last_updated_timestamp_seconds_metric = GaugeMetricFamily(
        name="celerymon_redis_last_updated_timestamp_seconds",
        documentation="Timestamp that the redis inspection data is updated.",
        unit="seconds",
    )
    queue_item_count_metric = GaugeMetricFamily(
        name="celerymon_redis_queue_item_count",
        documentation="Number of tasks held in queues",
        labels=["queue_name", "priority"],
    )
    if watcher.last_updated_timestamp is not None:
        last_updated_timestamp_seconds_metric.add_metric(
            labels=[],
            value=watcher.last_updated_timestamp.timestamp(),
            timestamp=watcher.last_updated_timestamp.timestamp(),
        )
        for key, length in watcher.queue_item_count.items():
            queue_name, priority = key
            queue_item_count_metric.add_metric(
                labels=[queue_name, str(priority)],
                value=length,
                timestamp=watcher.last_updated_timestamp.timestamp(),
            )
    return [last_updated_timestamp_seconds_metric, queue_item_count_metric]


def worker_metrics(watcher: WorkerWatcher) -> list[prometheus_client.Metric]:
    last_updated_timestamp_seconds_metric = GaugeMetricFamily(
        name="celerymon_inspect_last_updated_timestamp_seconds",
        documentation="Timestamp that the worker inspection data is updated.",
        unit="seconds",
    )
    oldest_started_task_timestamp_seconds_metric = GaugeMetricFamily(
        name="celerymon_inspect_oldest_started_task_timestamp_seconds",
        documentation="Oldest timestamps for each task name for all actively running tasks",
        labels=["task_name"],
        unit="seconds",
    )
    active_task_count_metric = GaugeMetricFamily(
        name="celerymon_inspect_worker_held_task_count",
        documentation="Number of tasks held in all workers",
        labels=["task_name", "state", "hostname"],
    )
    if watcher.last_updated_timestamp is not None:
        last_updated_timestamp_seconds_metric.add_metric(
            labels=[],
            value=watcher.last_updated_timestamp.timestamp(),
            timestamp=watcher.last_updated_timestamp.timestamp(),
        )
        for task_name, timestamp in watcher.oldest_started_task_timestamp.items():
            oldest_started_task_timestamp_seconds_metric.add_metric(
                labels=[task_name],
                value=timestamp.timestamp(),
                timestamp=watcher.last_updated_timestamp.timestamp(),
            )
        for key, count in watcher.task_count.items():
            state, task_name, hostname = key
            active_task_count_metric.add_metric(
                labels=[task_name, state, hostname],
                value=count,
                timestamp=watcher.last_updated_timestamp.timestamp(),
            )
    return [
        last_updated_timestamp_seconds_metric,
        oldest_started_task_timestamp_seconds_metric,
        active_task_count_metric,
    ]


def event_metrics(watcher: EventWatcher) -> list[prometheus_client.Metric]:
    last_received_timestamp_seconds_metric = GaugeMetricFamily(
        name="celerymon_events_last_received_timestamp_seconds",
        documentation="Last received timestamps for each task name and event name.",
        labels=["task_name", "event_name"],
        unit="seconds",
    )
    events_count_metric = CounterMetricFamily(
        name="celerymon_events_count",
        documentation="The task event count per task name and event name.",
        labels=["task_name", "event_name"],
    )
    task_runtime_seconds_metric = HistogramMetricFamily(
        name="celerymon_events_task_runtime_seconds",
        documentation=(
            "Task runtime per task name, labeled by result (success|failed)."
        ),
        labels=["task_name", "result"],
        unit="seconds",
    )
    online_worker_count_metric = GaugeMetricFamily(
        name="celerymon_events_online_worker_count",
        documentation=(
            "Number of workers currently online, derived from worker-online, "
            "worker-heartbeat, and worker-offline events."
        ),
    )
    queue_wait_seconds_metric = HistogramMetricFamily(
        name="celerymon_events_queue_wait_seconds",
        documentation=(
            "Queue wait time (task-sent to task-started) per task name, in seconds."
        ),
        labels=["task_name"],
        unit="seconds",
    )
    oldest_queued_task_age_seconds_metric = GaugeMetricFamily(
        name="celerymon_events_oldest_queued_task_age_seconds",
        documentation=(
            "Age of the oldest in-flight task per queue (sent but not yet started), "
            "in seconds."
        ),
        labels=["queue_name"],
        unit="seconds",
    )
    in_flight_evicted_metric = CounterMetricFamily(
        name="celerymon_events_in_flight_evicted",
        documentation=(
            "Count of in-flight entries evicted by reason: failed, revoked, "
            "expired, ttl (exceeded max age), lru (cache cap hit)."
        ),
        labels=["reason"],
    )
    in_flight_cache_size_metric = GaugeMetricFamily(
        name="celerymon_events_in_flight_cache_size",
        documentation="Current size of the in-flight uuid correlation cache.",
    )
    if watcher.last_received_timestamp is not None:
        for key, timestamp in watcher.last_received_timestamp_per_task_event.items():
            count = watcher.num_events_per_task_count[key]

            task_name, event_name = key
            last_received_timestamp_seconds_metric.add_metric(
                labels=[task_name, event_name],
                value=timestamp.timestamp(),
                timestamp=timestamp.timestamp(),
            )
            events_count_metric.add_metric(
                labels=[task_name, event_name],
                value=count,
                timestamp=timestamp.timestamp(),
            )
        for task_name in watcher.task_names:
            for result in ("success", "failed"):
                key = (task_name, result)
                acc = 0.0
                buckets = []
                for i, bound in enumerate(watcher.upper_bounds):
                    acc += watcher.task_runtime_sec[i].get(key, 0)
                    buckets.append((floatToGoString(bound), acc))
                task_runtime_seconds_metric.add_metric(
                    labels=[task_name, result],
                    buckets=buckets,
                    sum_value=watcher.task_runtime_sec_sum.get(key, 0.0),
                    timestamp=watcher.last_received_timestamp.timestamp(),
                )
            queue_wait_acc = 0.0
            queue_wait_buckets_values: list[tuple[str, float]] = []
            for i, bound in enumerate(watcher.queue_wait_upper_bounds):
                queue_wait_acc += watcher.queue_wait_sec[i].get(task_name, 0)
                queue_wait_buckets_values.append(
                    (floatToGoString(bound), queue_wait_acc)
                )
            queue_wait_seconds_metric.add_metric(
                labels=[task_name],
                buckets=queue_wait_buckets_values,
                sum_value=watcher.queue_wait_sec_sum.get(task_name, 0.0),
                timestamp=watcher.last_received_timestamp.timestamp(),
            )
        now = datetime.datetime.now(tz=datetime.UTC)
        online_worker_count_metric.add_metric(
            labels=[],
            value=watcher.online_worker_count(now),
            timestamp=watcher.last_received_timestamp.timestamp(),
        )
        for queue_name, age in watcher.oldest_queued_age_by_queue(now).items():
            oldest_queued_task_age_seconds_metric.add_metric(
                labels=[queue_name],
                value=age,
                timestamp=watcher.last_received_timestamp.timestamp(),
            )
        for reason, count in watcher.eviction_counts().items():
            in_flight_evicted_metric.add_metric(
                labels=[reason],
                value=count,
                timestamp=watcher.last_received_timestamp.timestamp(),
            )
        in_flight_cache_size_metric.add_metric(
            labels=[],
            value=watcher.in_flight_cache_size(),
            timestamp=watcher.last_received_timestamp.timestamp(),
        )
    return [
        last_received_timestamp_seconds_metric,
        events_count_metric,
        task_runtime_seconds_metric,
        online_worker_count_metric,
        queue_wait_seconds_metric,
        oldest_queued_task_age_seconds_metric,
        in_flight_evicted_metric,
        in_flight_cache_size_metric,
    ]
