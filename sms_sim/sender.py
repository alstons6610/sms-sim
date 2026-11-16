"""Senders: the workers that consume messages and simulate delivery.

Each Sender owns a private `random.Random`. Nothing random is shared between
senders, which means two things at once: no lock contention on the hot path,
and draws that cannot interleave unpredictably between concurrent tasks.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, List, Optional

from .config import derive_rng
from .messages import Message

# Placed on the queue once per sender to tell it there is no more work.
SHUTDOWN = None

SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], float]


@dataclass(frozen=True)
class SendResult:
    """The outcome of one simulated send attempt."""

    sender_id: int
    message_id: int
    ok: bool
    elapsed: float


def assign_failure_rates(
    num_senders: int, mean_rate: float, rng: random.Random
) -> List[float]:
    """Draw one failure rate per sender, uniformly spread around `mean_rate`.

    Rates are drawn in sender-index order from a single dedicated generator
    before any sender starts, so sender i always gets the same rate for a given
    seed no matter how the event loop schedules things.

    Uniform(0, 2 * mean_rate) has mean exactly `mean_rate`. Clamping to [0, 1]
    only bites when mean_rate > 0.5, where it pulls the realised mean down.
    """
    return [min(1.0, max(0.0, rng.uniform(0.0, 2.0 * mean_rate))) for _ in range(num_senders)]


class Sender:
    """One simulated SMS gateway with its own latency draws and failure rate."""

    def __init__(
        self,
        sender_id: int,
        failure_rate: float,
        mean_wait: float,
        rng: random.Random,
        sleep: SleepFn = asyncio.sleep,
        clock: ClockFn = time.monotonic,
    ) -> None:
        self.sender_id = sender_id
        self.failure_rate = failure_rate
        self.mean_wait = mean_wait
        self.rng = rng
        # Injected so tests can run with a fake clock instead of real waiting.
        self._sleep = sleep
        self._clock = clock

    async def send(self, message: Message) -> SendResult:
        """Simulate sending one message.

        Consumes exactly two draws from this sender's stream, always in the
        same order: the wait time, then the failure roll. Keeping that count
        fixed is what stops the stream drifting out of alignment between runs.
        """
        wait = self.rng.expovariate(1.0 / self.mean_wait)
        roll = self.rng.random()

        started = self._clock()
        # A failed send still costs time: you wait, and then it fails.
        await self._sleep(wait)
        elapsed = self._clock() - started

        return SendResult(
            sender_id=self.sender_id,
            message_id=message.id,
            ok=roll >= self.failure_rate,
            elapsed=elapsed,
        )


async def run_sender(sender: Sender, queue: "asyncio.Queue", stats) -> None:
    """Pull messages until the shutdown sentinel arrives, recording each result."""
    while True:
        message: Optional[Message] = await queue.get()
        if message is SHUTDOWN:
            return
        stats.record(await sender.send(message))


def build_senders(
    num_senders: int,
    failure_rates: List[float],
    mean_wait: float,
    seed: int,
    sleep: SleepFn = asyncio.sleep,
    clock: ClockFn = time.monotonic,
) -> List[Sender]:
    """Create the sender pool, giving each member its own derived generator."""
    return [
        Sender(
            sender_id=i,
            failure_rate=failure_rates[i],
            mean_wait=mean_wait,
            rng=derive_rng(seed, "sender:{}".format(i)),
            sleep=sleep,
            clock=clock,
        )
        for i in range(num_senders)
    ]
