from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


class RepeatTimer(threading.Timer):
    """Timer that repeats forever."""

    def run(self):
        name = getattr(self.function, "__qualname__", repr(self.function))
        failing = False
        while not self.finished.wait(self.interval):
            try:
                self.function(*self.args, **self.kwargs)
            except Exception:
                if not failing:
                    logger.exception(
                        "%s failed, retrying every %.1fs until it recovers",
                        name,
                        self.interval,
                    )
                    failing = True
            else:
                if failing:
                    logger.warning("%s recovered", name)
                    failing = False
