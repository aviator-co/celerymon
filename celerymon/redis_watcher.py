# Copyright 2024 Aviator Technologies, Inc.
# SPDX-License-Identifier: MIT

from __future__ import annotations

import datetime
from typing import Iterable

import redis

from .timer import RepeatTimer

PRIORITY_SEP = "\x06\x16"
PRIORITY_STEPS = [0, 3, 6, 9]


class RedisWatcher:
    last_updated_timestamp: datetime.datetime | None
    queue_item_count: dict[tuple[str, int], int]

    @classmethod
    def create_started(
        cls,
        client: redis.StrictRedis,
        queues: Iterable[str],
        interval: float,
    ) -> RedisWatcher:
        watcher = cls(client, queues)

        timer = RepeatTimer(interval, watcher._update)
        timer.start()

        return watcher

    def __init__(self, client: redis.StrictRedis, queues: Iterable[str]):
        self._client = client
        self._queues = queues
        self.last_updated_timestamp = None
        self.queue_item_count = dict()

    def _update(self) -> None:
        lengths: dict[tuple[str, int], int] = dict()
        for queue_name in self._queues:
            for priority in PRIORITY_STEPS:
                key = self._format_key(queue_name, priority)
                lengths[(queue_name, priority)] = self._client.llen(key)

        self.last_updated_timestamp = datetime.datetime.now(tz=datetime.UTC)
        self.queue_item_count = lengths

    def _format_key(self, queue_name: str, priority: int) -> str:
        """
        Format the Celery queue Redis key.

        When Redis backend is used for Celery, it uses one key for storing the
        pending tasks. Different priority items are stored in a different key. This
        returns the Redis key for a given queue name and a priority.

        See https://stackoverflow.com/a/43420719 for details.
        """
        if not priority:
            return queue_name
        return "{0}{1}{2}".format(queue_name, PRIORITY_SEP, priority)
