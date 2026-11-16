"""Metrics collection and the periodic progress monitor.

A single shared Stats object is safe here without any lock. asyncio runs every
coroutine on one thread and only switches at an `await`, and `record` contains
no await, so an increment can never be interrupted halfway through. Under
threads this would need a lock; under asyncio it does not.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from .sender import SendResult

SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], float]


class Stats:
    """Running totals for the whole run, plus the derived progress metrics."""

    def __init__(self, clock: ClockFn = time.monotonic) -> None:
        self._clock = clock
        self.started_at = clock()
        self.sent = 0
        self.failed = 0
        self.total_elapsed = 0.0

    def record(self, result: SendResult) -> None:
        """Fold one send outcome into the totals."""
        if result.ok:
            self.sent += 1
        else:
            self.failed += 1
        self.total_elapsed += result.elapsed

    @property
    def processed(self) -> int:
        """Messages handled, counting failures."""
        return self.sent + self.failed

    @property
    def average_send_time(self) -> float:
        """Mean wall-clock seconds per message, counting failures."""
        if self.processed == 0:
            return 0.0
        return self.total_elapsed / self.processed

    @property
    def elapsed(self) -> float:
        """Seconds since the run started."""
        return self._clock() - self.started_at

    @property
    def throughput(self) -> float:
        """Messages processed per second, averaged since the start of the run."""
        elapsed = self.elapsed
        if elapsed <= 0:
            return 0.0
        return self.processed / elapsed

    def format_line(self) -> str:
        """One line of progress output."""
        return (
            "[{:7.1f}s] sent={:>7}  failed={:>7}  "
            "avg={:.4f}s  throughput={:>9.1f} msg/s".format(
                self.elapsed,
                self.sent,
                self.failed,
                self.average_send_time,
                self.throughput,
            )
        )


async def run_monitor(
    stats: Stats,
    interval: float,
    sleep: SleepFn = asyncio.sleep,
    emit: Callable[[str], None] = print,
) -> None:
    """Print a progress line every `interval` seconds until cancelled.

    The monitor only reads Stats; it never takes part in sending, so changing
    the interval cannot affect the throughput it reports.
    """
    while True:
        await sleep(interval)
        emit(stats.format_line())
