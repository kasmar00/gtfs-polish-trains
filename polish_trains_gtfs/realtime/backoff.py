# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
import time
from collections.abc import Callable
from datetime import timedelta

import requests

logger = logging.getLogger("Backoff")


class BackoffRequired(ValueError):
    def __init__(self, context: str = "", force_pause_s: float = 0.0) -> None:
        super().__init__(context)
        self.force_pause_s = force_pause_s

    @classmethod
    def check_api_response(cls, r: requests.Response) -> None:
        if r.status_code == 500:
            raise cls(f"{r.url}: 500 Internal Server Error")
        elif r.status_code == 429:
            raise cls(
                f"{r.url}: 429 Too Many Requests",
                force_pause_s=(
                    pause
                    if "X-Ratelimit-Reset" in r.headers
                    and 0 <= (pause := int(r.headers["X-Ratelimit-Reset"])) <= 3600
                    else 0.0
                ),
            )


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

    def sleep(self, force_min_sleep: float = 0.0) -> None:
        now = time.monotonic()
        until = self.last_run + self.pause
        if force_min_sleep >= 0 or now < until:
            sleep = max(force_min_sleep, until - now)
            time.sleep(sleep)

    def loop(self, callback: Callable[[], None]) -> None:
        while True:
            force_min_pause = 0.0

            try:
                self.start_run()
                callback()
                self.recalculate_pause(failed=False)
            except BackoffRequired as e:
                self.recalculate_pause(failed=True)
                force_min_pause = e.force_pause_s
                logger.error(
                    "Backing off for %s: %s",
                    format_period(max(self.pause, force_min_pause)),
                    e.args[0],
                )

            self.sleep(force_min_pause)


def format_period(seconds: float) -> str:
    return str(timedelta(seconds=seconds))
