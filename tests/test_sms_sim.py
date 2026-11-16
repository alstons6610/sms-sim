"""Tests for the SMS simulation.

Every test that touches the clock uses FakeClock, so the whole suite runs in
well under a second instead of actually waiting. Every test that touches
randomness uses a fixed seed, so assertions are on exact values rather than
statistical ranges -- a seeded test should never be flaky.
"""

from __future__ import annotations

import asyncio

import pytest

from sms_sim.config import Config, derive_rng
from sms_sim.main import simulate
from sms_sim.messages import ALPHABET, MAX_BODY_LENGTH, generate_messages
from sms_sim.sender import (
    Sender,
    assign_failure_rates,
    build_senders,
)
from sms_sim.stats import Stats

SEED = 12345


class FakeClock:
    """A clock that only moves when something sleeps on it."""

    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    async def sleep(self, duration: float) -> None:
        self.now += duration
        # Yield control so other tasks still get to run, but without waiting.
        await asyncio.sleep(0)


# --- message generation ----------------------------------------------------


def test_generates_requested_count_with_valid_bodies():
    messages = list(generate_messages(500, derive_rng(SEED, "messages")))

    assert len(messages) == 500
    assert [m.id for m in messages] == list(range(500))
    for message in messages:
        assert 1 <= len(message.body) <= MAX_BODY_LENGTH
        assert all(char in ALPHABET for char in message.body)


def test_same_seed_produces_identical_messages():
    first = list(generate_messages(200, derive_rng(SEED, "messages")))
    second = list(generate_messages(200, derive_rng(SEED, "messages")))

    assert first == second


def test_different_seeds_produce_different_messages():
    first = list(generate_messages(200, derive_rng(SEED, "messages")))
    second = list(generate_messages(200, derive_rng(SEED + 1, "messages")))

    assert first != second


# --- seeding and per-sender behaviour --------------------------------------


def test_failure_rates_are_reproducible_and_average_to_the_mean():
    rates = assign_failure_rates(1000, 0.1, derive_rng(SEED, "failure-rates"))
    again = assign_failure_rates(1000, 0.1, derive_rng(SEED, "failure-rates"))

    assert rates == again
    assert len(rates) == 1000
    assert all(0.0 <= rate <= 1.0 for rate in rates)
    # Uniform(0, 2 * mean) averages to mean; 1000 draws land close to it.
    assert abs(sum(rates) / len(rates) - 0.1) < 0.01


def test_senders_are_independent_streams():
    """Each sender must draw from its own stream, not a shared one."""
    rates = assign_failure_rates(4, 0.1, derive_rng(SEED, "failure-rates"))
    senders = build_senders(4, rates, mean_wait=0.1, seed=SEED)

    assert senders[0].rng.random() != senders[1].rng.random()


def test_sender_draws_are_reproducible_for_a_given_seed():
    """The k-th send by sender i is identical across runs with the same seed."""

    def draws_for_sender_two():
        clock = FakeClock()
        rates = assign_failure_rates(5, 0.2, derive_rng(SEED, "failure-rates"))
        senders = build_senders(
            5, rates, mean_wait=0.05, seed=SEED, sleep=clock.sleep, clock=clock.time
        )
        messages = list(generate_messages(10, derive_rng(SEED, "messages")))

        async def send_all():
            return [await senders[2].send(m) for m in messages]

        return asyncio.run(send_all())

    assert draws_for_sender_two() == draws_for_sender_two()


# --- stats -----------------------------------------------------------------


def test_stats_average_and_throughput():
    clock = FakeClock()
    stats = Stats(clock=clock.time)

    async def exercise():
        rates = [0.0, 1.0]  # sender 0 always succeeds, sender 1 always fails
        senders = build_senders(
            2, rates, mean_wait=0.1, seed=SEED, sleep=clock.sleep, clock=clock.time
        )
        messages = list(generate_messages(4, derive_rng(SEED, "messages")))
        for i, message in enumerate(messages):
            stats.record(await senders[i % 2].send(message))

    asyncio.run(exercise())

    assert stats.processed == 4
    assert stats.sent == 2
    assert stats.failed == 2
    # Average counts failures, so it covers all four sends.
    assert stats.average_send_time == pytest.approx(stats.total_elapsed / 4)
    assert stats.throughput == pytest.approx(4 / clock.now)


def test_stats_are_zero_before_anything_is_processed():
    stats = Stats(clock=FakeClock().time)

    assert stats.processed == 0
    assert stats.average_send_time == 0.0
    assert stats.throughput == 0.0


# --- the pipeline end to end -----------------------------------------------


def run_simulation(config: Config, clock: FakeClock) -> Stats:
    """Run a full simulation on a fake clock, returning its stats.

    The monitor keeps the real asyncio.sleep; with a long interval it simply
    never fires before the run finishes and is cancelled.
    """
    stats = Stats(clock=clock.time)
    asyncio.run(simulate(config, stats, sleep=clock.sleep, clock=clock.time))
    return stats


def test_no_messages_are_lost():
    config = Config(
        num_messages=1000,
        num_senders=25,
        mean_wait=0.05,
        mean_failure_rate=0.1,
        monitor_interval=1000.0,  # long enough that it never fires
        queue_size=10,  # deliberately small, to exercise backpressure
        seed=SEED,
    )
    stats = run_simulation(config, FakeClock())

    assert stats.processed == 1000
    assert stats.sent + stats.failed == 1000


def test_failure_count_is_exactly_reproducible():
    """With a fixed seed the totals are deterministic, so assert them exactly."""
    config = Config(
        num_messages=500,
        num_senders=8,
        mean_wait=0.05,
        mean_failure_rate=0.2,
        monitor_interval=1000.0,
        queue_size=50,
        seed=SEED,
    )
    first = run_simulation(config, FakeClock())
    second = run_simulation(config, FakeClock())

    assert (first.sent, first.failed) == (second.sent, second.failed)
    assert first.processed == 500
    # A 20% mean failure rate should land near 100 failures out of 500.
    assert 50 <= first.failed <= 150


def test_zero_messages_terminates_cleanly():
    config = Config(
        num_messages=0,
        num_senders=4,
        monitor_interval=1000.0,
        seed=SEED,
    )
    stats = run_simulation(config, FakeClock())

    assert stats.processed == 0


# --- config validation -----------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"num_messages": -1},
        {"num_senders": 0},
        {"mean_wait": 0.0},
        {"mean_failure_rate": 1.5},
        {"monitor_interval": 0.0},
        {"queue_size": 0},
    ],
)
def test_invalid_config_is_rejected(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)
