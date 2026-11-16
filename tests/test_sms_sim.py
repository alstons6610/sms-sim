"""Tests for the SMS simulation.

Two ideas make this suite fast and reliable:

1. A fake clock. Nothing ever really sleeps, so a test that simulates
   1,000 messages at 0.05 seconds each finishes in milliseconds instead of
   taking almost a minute.

2. A fixed seed everywhere. Since the randomness is reproducible, tests can
   check exact values instead of ranges like "roughly 10% should fail". A
   seeded test should never pass one time and fail the next.

Run with:  python -m pytest tests/ -q
"""

from __future__ import annotations

import asyncio

import pytest

from sms_sim.config import Config, derive_rng
from sms_sim.main import simulate
from sms_sim.messages import ALPHABET, MAX_BODY_LENGTH, generate_messages
from sms_sim.sender import assign_failure_rates, build_senders
from sms_sim.stats import Stats

# One shared seed for the whole file, so every test is reproducible.
SEED = 12345


class FakeClock:
    """A stand-in for the real clock that only moves when something sleeps.

    The real code calls `sleep(0.1)` and waits a tenth of a second. This
    version just adds 0.1 to its own counter and returns immediately, so
    "time passes" without any real waiting.

    Pass `clock.time` and `clock.sleep` wherever the real `time.monotonic`
    and `asyncio.sleep` would normally go.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        """Stands in for time.monotonic()."""
        return self.now

    async def sleep(self, duration: float) -> None:
        """Stands in for asyncio.sleep(), but instant."""
        self.now += duration

        # Still hand control back to the event loop briefly, so other senders
        # get a turn. Sleeping for 0 does that without any real delay. Without
        # this, one sender could hog the queue and the test wouldn't exercise
        # concurrency at all.
        await asyncio.sleep(0)


# --- message generation ----------------------------------------------------


def test_generates_requested_count_with_valid_bodies():
    """Messages should match the spec: right count, 1-100 chars, valid ids."""
    messages = list(generate_messages(500, derive_rng(SEED, "messages")))

    assert len(messages) == 500
    assert [m.id for m in messages] == list(range(500))
    for message in messages:
        assert 1 <= len(message.body) <= MAX_BODY_LENGTH
        assert all(char in ALPHABET for char in message.body)


def test_same_seed_produces_identical_messages():
    """The core reproducibility promise: same seed in, same messages out."""
    first = list(generate_messages(200, derive_rng(SEED, "messages")))
    second = list(generate_messages(200, derive_rng(SEED, "messages")))

    assert first == second


def test_different_seeds_produce_different_messages():
    """The flip side: a different seed should give genuinely different output.

    Without this test, a bug that ignored the seed entirely would still pass
    the test above, since identical-every-time also means identical to itself.
    """
    first = list(generate_messages(200, derive_rng(SEED, "messages")))
    second = list(generate_messages(200, derive_rng(SEED + 1, "messages")))

    assert first != second


# --- seeding and per-sender behaviour --------------------------------------


def test_failure_rates_are_reproducible_and_average_to_the_mean():
    """Each sender gets its own rate, but they should average to the config."""
    rates = assign_failure_rates(1000, 0.1, derive_rng(SEED, "failure-rates"))
    again = assign_failure_rates(1000, 0.1, derive_rng(SEED, "failure-rates"))

    assert rates == again
    assert len(rates) == 1000
    assert all(0.0 <= rate <= 1.0 for rate in rates)

    # Uniform(0, 2 * mean) averages to the mean. Over 1,000 draws it lands
    # very close, so a 0.01 tolerance is safe and not flaky.
    assert abs(sum(rates) / len(rates) - 0.1) < 0.01


def test_senders_are_independent_streams():
    """Each sender must have its own generator, not share one.

    If all senders shared a generator, they'd return the same next number
    here. Sharing is the exact bug that breaks reproducibility, so this test
    guards against it directly.
    """
    rates = assign_failure_rates(4, 0.1, derive_rng(SEED, "failure-rates"))
    senders = build_senders(4, rates, mean_wait=0.1, seed=SEED)

    assert senders[0].rng.random() != senders[1].rng.random()


def test_sender_draws_are_reproducible_for_a_given_seed():
    """Sender i's k-th send should be identical across runs with the same seed.

    This is the guarantee the spec actually asks for: not that sender 2 gets
    the same messages every time, but that its own sequence of waits and
    failures never changes.
    """

    def draws_for_sender_two():
        """Run ten sends through sender 2 and return the results."""
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
    """Check the metric math against a clock whose exact value we know."""
    clock = FakeClock()
    stats = Stats(clock=clock.time)

    async def exercise():
        # Rates of 0.0 and 1.0 mean sender 0 always succeeds and sender 1
        # always fails. That removes the randomness so the counts are certain.
        rates = [0.0, 1.0]
        senders = build_senders(
            2, rates, mean_wait=0.1, seed=SEED, sleep=clock.sleep, clock=clock.time
        )
        messages = list(generate_messages(4, derive_rng(SEED, "messages")))

        # Alternate between the two senders, so we get 2 successes, 2 failures.
        for i, message in enumerate(messages):
            stats.record(await senders[i % 2].send(message))

    asyncio.run(exercise())

    assert stats.processed == 4
    assert stats.sent == 2
    assert stats.failed == 2

    # The average has to divide by 4, not 2: failures count toward the time.
    assert stats.average_send_time == pytest.approx(stats.total_elapsed / 4)

    # approx() is needed because floating point arithmetic rarely lands on an
    # exact value; comparing floats with == is unreliable.
    assert stats.throughput == pytest.approx(4 / clock.now)


def test_stats_are_zero_before_anything_is_processed():
    """Nothing processed yet shouldn't crash with a divide-by-zero."""
    stats = Stats(clock=FakeClock().time)

    assert stats.processed == 0
    assert stats.average_send_time == 0.0
    assert stats.throughput == 0.0


# --- the whole pipeline together -------------------------------------------


def run_simulation(config: Config, clock: FakeClock) -> Stats:
    """Helper: run a full simulation on the fake clock and return its stats.

    The monitor keeps the real asyncio.sleep. The tests below set a very long
    interval, so it simply never fires before the run finishes and gets
    cancelled, which keeps progress output out of the test results.
    """
    stats = Stats(clock=clock.time)
    asyncio.run(simulate(config, stats, sleep=clock.sleep, clock=clock.time))
    return stats


def test_no_messages_are_lost():
    """Every message produced has to be accounted for as sent or failed.

    This is the most important test in the file. It's the one that would catch
    a shutdown bug where senders exit while messages are still in the queue.
    """
    config = Config(
        num_messages=1000,
        num_senders=25,
        mean_wait=0.05,
        mean_failure_rate=0.1,
        monitor_interval=1000.0,  # long enough that it never fires
        queue_size=10,  # deliberately tiny, to force backpressure
        seed=SEED,
    )
    stats = run_simulation(config, FakeClock())

    assert stats.processed == 1000
    assert stats.sent + stats.failed == 1000


def test_failure_count_is_exactly_reproducible():
    """The same seed should give the same totals, run after run."""
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

    # Sanity check on the failure rate itself: a 20% mean across 500 messages
    # should land near 100. The window is wide because which sender gets which
    # message can shift, and senders have different rates.
    assert 50 <= first.failed <= 150


def test_zero_messages_terminates_cleanly():
    """An empty run should still shut down instead of hanging forever.

    Edge cases like this are where shutdown logic usually breaks.
    """
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
        {"mean_wait": 0.0},  # would cause a divide-by-zero in the sender
        {"mean_failure_rate": 1.5},
        {"monitor_interval": 0.0},
        {"queue_size": 0},
    ],
)
def test_invalid_config_is_rejected(kwargs):
    """Bad settings should fail immediately with a clear error.

    @parametrize runs this one test once per entry in the list above, so these
    count as six separate tests rather than one.
    """
    with pytest.raises(ValueError):
        Config(**kwargs)
