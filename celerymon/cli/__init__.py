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

import celerymon


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
    ts_reporter = celerymon.LastUpdatedTimestampReporter()

    celerymon.start_celerymon_worker_inspector(
        app, args.worker_inspect_interval_sec, ts_reporter=ts_reporter
    )
    celerymon.start_celerymon_event_receiver(
        app, buckets=buckets, ts_reporter=ts_reporter
    )
    celerymon.start_celerymon_redis_watcher(
        redis_client,
        args.redis_watch_interval_sec,
        args.queue,
        ts_reporter=ts_reporter,
    )

    app = prometheus_client.make_wsgi_app()

    def healthz_wrapper(
        environ: wsgiref.types.WSGIEnvironment,
        start_response: wsgiref.types.StartResponse,
    ) -> Iterable[bytes]:
        if environ["PATH_INFO"] == "/healthz":
            stat = check_health(ts_reporter, args.healthz_unhealthy_threshold_sec)
            if stat == "":
                start_response("200 OK", [("", "")])
                return [b"OK"]
            start_response("500 Internal Server Error", [("", "")])
            return [stat.encode("utf-8")]

        return app(environ, start_response)

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


def check_health(
    ts_reporter: celerymon.LastUpdatedTimestampReporter, threshold_sec: int
) -> str:
    now = datetime.datetime.utcnow()

    status = []
    if (
        not ts_reporter.redis_last_updated
        or (now - ts_reporter.redis_last_updated).seconds > threshold_sec
    ):
        status.append(f"Redis data is too old ({ts_reporter.redis_last_updated})")
    if (
        not ts_reporter.worker_inspect_last_updated
        or (now - ts_reporter.worker_inspect_last_updated).seconds > threshold_sec
    ):
        status.append(
            f"Worker inspection data is too old ({ts_reporter.worker_inspect_last_updated})"
        )
    if (
        not ts_reporter.event_last_updated
        or (now - ts_reporter.event_last_updated).seconds > threshold_sec
    ):
        status.append(f"Event data is too old ({ts_reporter.event_last_updated})")

    return "\r\n".join(status)
