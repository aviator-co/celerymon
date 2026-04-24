# celerymon

celerymon is a server that exposes Prometheus metrics for Celery tasks.

## Motivation

[Flower](https://github.com/mher/flower) and
[celery-exporter](https://github.com/danihodovic/celery-exporter) both expose
Prometheus metrics. We want to create yet another Celery metric exporter for the
same reason as celery-exporter.

## Mechanism

There are three ways to observe Celery:

* Using the underlying broker directly.

  As suggested in https://docs.celeryq.dev/en/stable/userguide/monitoring.html,
  this is the only way to measure the queue length.

* Using the Celery worker inspection API.

  Celery provides an RPC mechanism to talk to workers, and we can ask existing
  workers to check the currently executing tasks.

* Using the Celery events.

  Worker and task senders are optionally configured to send events. This
  requires an opt-in, but is real-time. celerymon maintains a lightweight
  UUID to task name cache (rather than using Celery's built-in State object)
  to keep memory usage bounded. If the connection drops, celerymon
  automatically reconnects with exponential backoff.

These three can cover different areas. By querying the broker directly, we can
get the queued items stats. By using the worker inspection API, we can get the
currently executing tasks. By using the Celery events, we can receive the stats
on the finished tasks. celerymon uses all these three to get the data.

## Limitations

* Because Celery events are sent only when workers and senders are configured
  so, if they are not configured, celerymon cannot receive events. For workers,
  celerymon periodically re-configures them to send events, but the senders are
  not configurable from it.

  While celerymon periodically re-configures workers to send events, relying on
  this means there can be a time gap between workers' start and reconfiguration.
  During this gap, the worker won't send events, which makes a hole in
  monitoring. If possible, configure workers to send events from the beginning.

* If the event stream connection drops, celerymon will automatically reconnect
  with exponential backoff (up to 60s). During the reconnection window, events
  will be missed.

* This is not specific to celerymon, but in general, this type of monitoring
  tools are not suited for short-lived ephemeral containers, such as Cloud Run.
  The background threads need to run continuously instead of just at the time a
  request is coming.

* Right now, part of celerymon is coupled to Redis as a broker. Looking at
  celery-exporter's code, there are ways to make this support more broker types.
  Just because we use Redis as a broker, currently only supports Redis.

## Metrics

| Name                                                      | Type      | Labels                           |
|-----------------------------------------------------------|-----------|----------------------------------|
| `celerymon_redis_last_updated_timestamp_seconds`          | Gauge     |                                  |
| `celerymon_redis_queue_item_count`                        | Gauge     | `queue_name`, `priority`         |
| `celerymon_inspect_last_updated_timestamp_seconds`        | Gauge     |                                  |
| `celerymon_inspect_oldest_started_task_timestamp_seconds` | Gauge     | `task_name`                      |
| `celerymon_inspect_worker_held_task_count`                | Gauge     | `task_name`, `state`, `hostname` |
| `celerymon_events_last_received_timestamp_seconds`        | Gauge     | `task_name`, `event_name`        |
| `celerymon_events_count`                                  | Counter   | `task_name`, `event_name`        |
| `celerymon_events_task_runtime_seconds`                   | Histogram | `task_name`, `result`            |
| `celerymon_events_online_worker_count`                    | Gauge     |                                  |
| `celerymon_events_queue_wait_seconds`                     | Histogram | `task_name`                      |
| `celerymon_events_oldest_queued_task_age_seconds`         | Gauge     | `queue_name`                     |
| `celerymon_events_in_flight_evicted`                      | Counter   | `reason`                         |
| `celerymon_events_in_flight_cache_size`                   | Gauge     |                                  |

There are timestamp metrics. These are meant to be used for checking the
monitoring health. If this stops updating, it means that the monitoring cannot
receive signals from those sources.

See the source code for the metric descriptions as well.

## Usage

```
celerymon --broker-url=BROKER_URL
          --queue=QUEUE_NAME [--queue=QUEUE_NAME ...]
          [--worker-inspect-interval-sec=10]
          [--redis-watch-interval-sec=10]
          [--healthz-unhealthy-threshold-sec=300]
          [--success-task-runtime-buckets=BUCKETS]
          [--queue-wait-buckets=BUCKETS]
          [--in-flight-cache-size=100000]
          [--in-flight-ttl-sec=3600]
          [--port=8000]
```

To monitor multiple queues, repeat the `--queue` flag:

```
celerymon --broker-url=redis://localhost:6379/0 --queue=celery --queue=agent
```

`--success-task-runtime-buckets` defines the histogram buckets for task
runtime. Defaults to log-uniform buckets spanning 10ms to ~50 minutes
(`0.01,0.03,0.1,0.3,1,3,10,30,100,300,1000,3000`), chosen to capture both
fast webhook-style tasks and long-running batch work.

`--queue-wait-buckets` defines the histogram buckets for queue wait time
(time from task-sent to task-started). Defaults to log-uniform buckets
spanning 3ms to ~17 minutes (`0.003,0.01,0.03,0.1,0.3,1,3,10,30,100,300,1000`),
with extra sub-10ms resolution for distinguishing healthy queue states.

`--in-flight-cache-size` caps the number of in-flight uuid→timestamp entries
held for correlating task-sent with task-started. When the cap is hit, the
oldest entry is evicted with reason `lru`.

`--in-flight-ttl-sec` is the max age of an in-flight entry before it is
treated as a zombie (task sent but never started or revoked) and evicted with
reason `ttl`. Tasks whose `expires` timestamp passes are evicted earlier with
reason `expired`. Eviction runs on a background timer every 30 seconds; metric
scrapes stay pure reads.

### Note on /healthz

celerymon serves /healthz as a health check endpoint that reacts to data source
inactivity. While the event watcher now auto-reconnects, other data sources
(Redis, worker inspection) may still stall. celerymon uses three
data sources for providing the data. If one of them cannot be updated for
`--healthz-unhealthy-threshold-sec`, this endpoint returns 500. You can
configure your container management tool to restart the container based on this
endpoint.
