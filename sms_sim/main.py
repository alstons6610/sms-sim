"""The entry point: creates the producer, the senders, and the monitor, and
runs them all at the same time.

The flow is:

    producer  ->  bounded queue  ->  pool of senders  ->  shared Stats
                                                               ^
                                                      monitor reads from here

The producer puts messages in, senders take them out, and the monitor prints
progress on a timer. All three run concurrently on one thread via asyncio.
"""

from __future__ import annotations

import asyncio
import sys
import time
from typing import Iterable, Optional

from .config import Config, derive_rng, parse_args
from .messages import Message, generate_messages
from .sender import (
    SHUTDOWN,
    ClockFn,
    SleepFn,
    assign_failure_rates,
    build_senders,
    run_sender,
)
from .stats import Stats, run_monitor


async def run_producer(
    queue: "asyncio.Queue", messages: Iterable[Message], num_senders: int
) -> None:
    """Feed messages into the queue, then tell the senders to stop.

    The queue has a size limit, and `put` waits when it's full. That's
    deliberate: if the senders fall behind, the producer slows down to match
    them instead of filling memory with messages nobody has gotten to yet.
    This is called backpressure, and it's what lets the program handle millions
    of messages without using more memory than it does for a thousand.

    At the end we add one SHUTDOWN marker per sender. Each sender picks up
    exactly one and exits, so the pool shuts down cleanly with no leftovers.
    """
    for message in messages:
        await queue.put(message)

    for _ in range(num_senders):
        await queue.put(SHUTDOWN)


async def simulate(
    config: Config,
    stats: Stats,
    sleep: SleepFn = asyncio.sleep,
    clock: ClockFn = time.monotonic,
) -> None:
    """Set everything up and run one full simulation.

    `sleep` and `clock` get passed down to the senders. Normally they're the
    real ones, but tests pass fakes so a simulated 5-minute run finishes
    instantly.
    """
    # Step 1: decide each sender's failure rate. Done up front, in order, from
    # its own dedicated generator, so these are identical on every run.
    failure_rates = assign_failure_rates(
        config.num_senders,
        config.mean_failure_rate,
        derive_rng(config.seed, "failure-rates"),
    )

    # Step 2: build the senders. Each gets its own generator derived from the
    # seed and its index.
    senders = build_senders(
        config.num_senders,
        failure_rates,
        config.mean_wait,
        config.seed,
        sleep=sleep,
        clock=clock,
    )

    # Step 3: set up the message generator. Nothing is generated yet, since
    # this is lazy; messages get created as the producer asks for them.
    messages = generate_messages(
        config.num_messages, derive_rng(config.seed, "messages")
    )

    # Step 4: the queue connecting the producer to the senders. maxsize is what
    # makes the backpressure described above happen.
    queue: "asyncio.Queue" = asyncio.Queue(maxsize=config.queue_size)

    # Step 5: start the background tasks. create_task starts them running
    # immediately; it doesn't wait for them to finish.
    monitor = asyncio.create_task(run_monitor(stats, config.monitor_interval))
    workers = [asyncio.create_task(run_sender(s, queue, stats)) for s in senders]

    try:
        # Fill the queue and send the shutdown markers.
        await run_producer(queue, messages, config.num_senders)

        # Wait for every sender to finish draining the queue and exit.
        await asyncio.gather(*workers)
    finally:
        # The monitor loops forever, so it has to be cancelled explicitly.
        # The workers are cancelled too, which normally does nothing since
        # they've already finished, but matters if we got here because of an
        # error or a Ctrl-C partway through.
        monitor.cancel()
        for worker in workers:
            worker.cancel()

        # Wait for the cancellations to actually land. return_exceptions=True
        # stops the expected "you were cancelled" errors from being raised here.
        await asyncio.gather(monitor, *workers, return_exceptions=True)


def print_header(config: Config) -> None:
    """Print the settings for this run, including the seed needed to repeat it."""
    print("sms-sim: {} messages, {} senders, mean wait {}s, mean failure rate {}".format(
        config.num_messages, config.num_senders, config.mean_wait, config.mean_failure_rate
    ))
    print("seed: {}  (re-run with --seed {} to reproduce)".format(config.seed, config.seed))
    print("-" * 78)


def print_summary(stats: Stats) -> None:
    """Print the final totals once everything is done."""
    print("-" * 78)
    print("done in {:.2f}s".format(stats.elapsed))
    print("  processed:  {}".format(stats.processed))
    print("  sent:       {}".format(stats.sent))
    print("  failed:     {}".format(stats.failed))
    print("  avg send:   {:.4f}s".format(stats.average_send_time))
    print("  throughput: {:.1f} msg/s".format(stats.throughput))


def main(argv: Optional[list] = None) -> int:
    """Read the settings, run the simulation, print the results.

    Returns the exit code: 0 for a normal finish, 130 for a Ctrl-C (which is
    the standard code for "interrupted by the user").
    """
    config = parse_args(argv)
    print_header(config)

    # Stats is created out here rather than inside simulate() so that a Ctrl-C
    # partway through can still print the totals collected so far.
    stats = Stats()

    try:
        # asyncio.run starts the event loop, runs the simulation, and cleans up.
        asyncio.run(simulate(config, stats))
    except KeyboardInterrupt:
        print("\ninterrupted")
        print_summary(stats)
        return 130

    print_summary(stats)
    return 0


# Only runs when this file is executed directly, not when it's imported.
if __name__ == "__main__":
    sys.exit(main())
