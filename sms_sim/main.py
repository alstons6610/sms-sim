"""Entry point: wires the producer, the sender pool, and the monitor together."""

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
    """Stream messages into the queue, then one shutdown sentinel per sender.

    `queue.put` blocks once the queue is full, so a slow pool pushes back on
    the producer instead of letting unsent messages pile up in memory.
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
    """Run one full simulation against the given config.

    `sleep` and `clock` are injected so tests can drive the senders with a fake
    clock and finish instantly instead of waiting out real send times.
    """
    failure_rates = assign_failure_rates(
        config.num_senders,
        config.mean_failure_rate,
        derive_rng(config.seed, "failure-rates"),
    )
    senders = build_senders(
        config.num_senders,
        failure_rates,
        config.mean_wait,
        config.seed,
        sleep=sleep,
        clock=clock,
    )
    messages = generate_messages(
        config.num_messages, derive_rng(config.seed, "messages")
    )

    queue: "asyncio.Queue" = asyncio.Queue(maxsize=config.queue_size)
    monitor = asyncio.create_task(run_monitor(stats, config.monitor_interval))
    workers = [asyncio.create_task(run_sender(s, queue, stats)) for s in senders]

    try:
        await run_producer(queue, messages, config.num_senders)
        await asyncio.gather(*workers)
    finally:
        monitor.cancel()
        for worker in workers:
            worker.cancel()
        await asyncio.gather(monitor, *workers, return_exceptions=True)


def print_header(config: Config) -> None:
    print("sms-sim: {} messages, {} senders, mean wait {}s, mean failure rate {}".format(
        config.num_messages, config.num_senders, config.mean_wait, config.mean_failure_rate
    ))
    print("seed: {}  (re-run with --seed {} to reproduce)".format(config.seed, config.seed))
    print("-" * 78)


def print_summary(stats: Stats) -> None:
    print("-" * 78)
    print("done in {:.2f}s".format(stats.elapsed))
    print("  processed:  {}".format(stats.processed))
    print("  sent:       {}".format(stats.sent))
    print("  failed:     {}".format(stats.failed))
    print("  avg send:   {:.4f}s".format(stats.average_send_time))
    print("  throughput: {:.1f} msg/s".format(stats.throughput))


def main(argv: Optional[list] = None) -> int:
    config = parse_args(argv)
    print_header(config)

    stats = Stats()
    try:
        asyncio.run(simulate(config, stats))
    except KeyboardInterrupt:
        print("\ninterrupted")
        print_summary(stats)
        return 130

    print_summary(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
