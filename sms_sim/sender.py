"""The senders: workers that take messages off the queue and "send" them.

A send is simulated. The sender waits a random amount of time, then flips a
weighted coin to decide whether the send succeeded or failed.

The important design point here: every sender gets its OWN random number
generator. Nothing random is shared. That matters because senders run at the
same time, and if they all pulled from one shared generator, the order they
pulled in would change from run to run, so the results would too.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, List, Optional

from .config import derive_rng
from .messages import Message

# Put on the queue once per sender to tell that sender "no more work, you can
# stop now". Using None keeps it simple, since a real message is never None.
SHUTDOWN = None

# These two are just names for function types, to make the signatures readable.
# SleepFn is "a function that takes seconds and can be awaited".
# ClockFn is "a function that takes nothing and returns the current time".
SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], float]


@dataclass(frozen=True)
class SendResult:
    """What happened on one send attempt.

    frozen=True means it can't be changed after it's created, which is what we
    want for a record of something that already happened.
    """

    sender_id: int
    message_id: int
    ok: bool  # True if the send succeeded, False if it failed
    elapsed: float  # how many seconds the attempt took


def assign_failure_rates(
    num_senders: int, mean_rate: float, rng: random.Random
) -> List[float]:
    """Give every sender its own failure rate, averaging out to `mean_rate`.

    Picking uniformly between 0 and 2x the mean gives an average of exactly the
    mean. So with a 10% mean, one sender might get 3% and another 17%.

    Two details that matter for reproducibility:
      - All the rates are drawn here, up front, before any sender starts.
      - They're drawn in order: sender 0's rate first, then sender 1's, etc.
    That's why sender 5 always ends up with the same rate for a given seed, no
    matter what order the senders actually run in.

    The min/max clamp keeps rates inside [0, 1]. It only ever kicks in if the
    mean is above 0.5, which would push 2x the mean past 1.
    """
    return [
        min(1.0, max(0.0, rng.uniform(0.0, 2.0 * mean_rate)))
        for _ in range(num_senders)
    ]


class Sender:
    """One simulated SMS gateway, with its own speed and its own failure rate."""

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
        self.failure_rate = failure_rate  # chance this sender fails, 0 to 1
        self.mean_wait = mean_wait  # average seconds per send
        self.rng = rng  # this sender's private random generator

        # sleep and clock are passed in instead of being called directly, so
        # that tests can swap in a fake clock. With the fake one, "waiting"
        # 0.1 seconds happens instantly, so the test suite runs in milliseconds
        # instead of minutes. In normal use these are just the real functions.
        self._sleep = sleep
        self._clock = clock

    async def send(self, message: Message) -> SendResult:
        """Pretend to send one message. Returns how it went.

        This always uses exactly two random numbers, always in the same order:
        first the wait time, then the failure roll. Keeping that count fixed is
        what keeps runs reproducible. If some code path ever used a different
        number of draws, this sender's sequence would shift and the run would
        stop matching previous runs.
        """
        # Exponential distribution: most sends are quick, a few take much
        # longer. That's roughly how real network latency behaves.
        # expovariate takes a rate, which is 1 divided by the average.
        wait = self.rng.expovariate(1.0 / self.mean_wait)

        # Roll a number between 0 and 1. If it lands below our failure rate,
        # this send fails. So a rate of 0.1 fails about 10% of the time.
        roll = self.rng.random()

        started = self._clock()

        # Note we wait BEFORE deciding success or failure. A failed send still
        # takes time in real life, the same way a request that times out still
        # made you wait for the timeout.
        await self._sleep(wait)

        # Measure the real elapsed time rather than reusing `wait`. These two
        # differ once the system is busy, and that difference is useful: it
        # shows how far behind the scheduler is running.
        elapsed = self._clock() - started

        return SendResult(
            sender_id=self.sender_id,
            message_id=message.id,
            ok=roll >= self.failure_rate,
            elapsed=elapsed,
        )


async def run_sender(sender: Sender, queue: "asyncio.Queue", stats) -> None:
    """Keep pulling messages off the queue and sending them, until told to stop.

    One of these runs per sender, all at the same time.
    """
    while True:
        # Waits here if the queue is empty, instead of spinning in a busy loop.
        message: Optional[Message] = await queue.get()

        # The producer puts one SHUTDOWN marker per sender at the end, so each
        # sender picks up exactly one and exits.
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
    """Create the whole pool of senders.

    Each one gets its own random generator, built from the seed plus its own
    index. So sender 3 always gets the generator for "<seed>:sender:3", which
    is why its behaviour is identical every run.
    """
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
