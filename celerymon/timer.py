from __future__ import annotations

import threading


class RepeatTimer(threading.Timer):
    """Timer that repeats forever."""

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
