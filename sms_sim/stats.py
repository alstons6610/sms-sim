"""Counting what happened, and printing progress while the run is going.

There's one shared Stats object that every sender writes to, with no lock
around it. That's safe here, and it's worth understanding why:

asyncio runs everything on a single thread. Tasks only hand control to each
other at an `await`. The `record` method below has no `await` in it, so once a
sender starts running it, it finishes before any other sender gets a turn.
Nothing can interrupt it halfway through.

If this used real threads instead, that guarantee would disappear and the
counters would need a lock.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from .sender import SendResult

SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], float]


class Stats:
    """Running totals for the whole run, plus the numbers we calculate from them."""

    def __init__(self, clock: ClockFn = time.monotonic) -> None:
        # The clock is passed in so tests can use a fake one. Defaults to
        # time.monotonic, which (unlike time.time) never jumps backwards if the
        # system clock is adjusted mid-run.
        self._clock = clock

        self.started_at = clock()
        self.sent = 0  # successful sends
        self.failed = 0  # failed sends
        self.total_elapsed = 0.0  # seconds spent across all attempts

    def record(self, result: SendResult) -> None:
        """Add one send result to the totals. Called once per message."""
        if result.ok:
            self.sent += 1
        else:
            self.failed += 1

        # Failed attempts count toward the time too, since they still waited.
        self.total_elapsed += result.elapsed

    # The @property decorator lets these be used like plain attributes
    # (stats.throughput, not stats.throughput()) even though they do work.

    @property
    def processed(self) -> int:
        """Total messages handled, successes and failures together."""
        return self.sent + self.failed

    @property
    def average_send_time(self) -> float:
        """Average seconds per message, including the ones that failed."""
        # Guard against dividing by zero before anything has been sent.
        if self.processed == 0:
            return 0.0
        return self.total_elapsed / self.processed

    @property
    def elapsed(self) -> float:
        """Seconds since the run started."""
        return self._clock() - self.started_at

    @property
    def throughput(self) -> float:
        """Messages per second, averaged over the whole run so far."""
        elapsed = self.elapsed
        if elapsed <= 0:
            return 0.0
        return self.processed / elapsed

    def format_line(self) -> str:
        """Build one line of progress output.

        The numbers in the format string set column widths, so the lines stay
        lined up as the counts grow. For example {:>7} means "right-align in 7
        characters" and {:.4f} means "4 digits after the decimal point".
        """
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
    """Print a progress line every `interval` seconds, forever.

    This loop has no exit condition on purpose. It runs as a background task,
    and main.py cancels it once all the messages are done.

    Note that the monitor only READS from stats. It never sends anything, so
    reporting more often can't slow the run down or change the numbers it
    reports.

    `emit` defaults to print but can be swapped out, which makes it easy to
    capture the output in a test instead of printing it.
    """
    while True:
        await sleep(interval)
        emit(stats.format_line())
