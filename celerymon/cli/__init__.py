# Copyright 2023 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

import argparse
import datetime
import wsgiref.simple_server
import wsgiref.types
from typing import Iterable, Sequence

import celery
import prometheus_client
import redis

from celerymon.event_watcher import EventWatcher
from celerymon.metrics import Collector
from celerymon.redis_watcher import RedisWatcher
from celerymon.worker_watcher import WorkerWatcher


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-url", type=str, required=True)
    parser.add_argument("--queue", action="append", required=True)
    parser.add_argument("--worker-inspect-interval-sec", type=int, default=10)
    parser.add_argument("--redis-watch-interval-sec", type=int, default=10)
    parser.add_argument("--healthz-unhealthy-threshold-sec", type=int, default=300)
    parser.add_argument("--success-task-runtime-buckets", type=str)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    buckets = parse_histogram_buckets(args.success_task_runtime_buckets)

    app = celery.Celery("", broker=args.broker_url)
    redis_client = redis.StrictRedis.from_url(args.broker_url)

    redis_watcher = RedisWatcher.create_started(
        redis_client, args.queue, args.redis_watch_interval_sec
    )
    worker_watcher = WorkerWatcher.create_started(app, args.worker_inspect_interval_sec)
    event_watcher = EventWatcher.create_started(
        app,
        # This has a wrong type annotation.
        app.events.State(),  # type: ignore[attr-defined]
        buckets,
    )
    collector = Collector(redis_watcher, worker_watcher, event_watcher)

    registry = prometheus_client.CollectorRegistry()
    registry.register(collector)

    prom_app = prometheus_client.make_wsgi_app(registry)

    def healthz_wrapper(
        environ: wsgiref.types.WSGIEnvironment,
        start_response: wsgiref.types.StartResponse,
    ) -> Iterable[bytes]:
        if environ["PATH_INFO"] == "/healthz":
            stat = _check_health(
                redis_watcher,
                worker_watcher,
                event_watcher,
                args.healthz_unhealthy_threshold_sec,
            )
            if stat == "":
                start_response("200 OK", [])
                return [b"OK"]
            start_response("500 Internal Server Error", [])
            return [stat.encode("utf-8")]

        return prom_app(environ, start_response)

    with wsgiref.simple_server.make_server("", args.port, healthz_wrapper) as httpd:
        print(f"Serving on port {args.port}...")
        httpd.serve_forever()


def parse_histogram_buckets(arg: str | None) -> Sequence[float]:
    if not arg:
        return prometheus_client.Histogram.DEFAULT_BUCKETS
    buckets = list()
    for bucket_str in arg.split(","):
        buckets.append(float(bucket_str.strip()))
    return tuple(buckets)


def _check_health(
    redis_watcher: RedisWatcher,
    worker_watcher: WorkerWatcher,
    event_watcher: EventWatcher,
    threshold_sec: int,
) -> str:
    now = datetime.datetime.now(tz=datetime.UTC)

    status = []
    if (
        redis_watcher.last_updated_timestamp
        and (now - redis_watcher.last_updated_timestamp).seconds > threshold_sec
    ):
        status.append(f"Redis data is too old ({redis_watcher.last_updated_timestamp})")
    if (
        worker_watcher.last_updated_timestamp
        and (now - worker_watcher.last_updated_timestamp).seconds > threshold_sec
    ):
        status.append(
            f"Worker inspection data is too old ({worker_watcher.last_updated_timestamp})"
        )
    if (
        event_watcher.last_received_timestamp
        and (now - event_watcher.last_received_timestamp).seconds > threshold_sec
    ):
        status.append(
            f"Event data is too old ({event_watcher.last_received_timestamp})"
        )

    return "\r\n".join(status)
