# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
import time
from collections.abc import Callable

import requests

logger = logging.getLogger("Backoff")


class Backoff:
    def __init__(self, period_s: int = 30, max_backoff_exponent: int = 8) -> None:
        self.period_s = period_s
        self.max_backoff_exponent = max_backoff_exponent

        self.pause = period_s
        self.last_run = 0.0
        self.failures = 0

    def start_run(self) -> None:
        self.last_run = time.monotonic()

    def recalculate_pause(self, failed: bool) -> None:
        if failed:
            self.pause = self.period_s * 2 ** min(self.failures, self.max_backoff_exponent)
            self.failures += 1
        else:
            self.pause = self.period_s
            self.failures = 0

    def sleep(self) -> None:
        now = time.monotonic()
        until = self.last_run + self.pause
        if now < until:
            time.sleep(until - now)

    def loop(self, callback: Callable[[], None]) -> None:
        while True:
            try:
                self.start_run()
                callback()
                self.recalculate_pause(failed=False)
            except requests.HTTPError as r:
                if r.response.status_code == 500:
                    self.recalculate_pause(failed=True)
                    logger.error(
                        "%s: 500 Internal Server Error: backing off for %d s",
                        r.response.url,
                        self.pause,
                    )
                else:
                    raise

            self.sleep()
