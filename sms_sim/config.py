"""Settings for a run, command-line parsing, and seed splitting.

This module owns the seed, so it also owns the rule for splitting that seed
into separate random generators. Every random number in the whole program
traces back to `derive_rng` below.
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass

# Defaults, kept as named constants so the CLI help text and the Config class
# can't drift apart.
DEFAULT_MESSAGES = 1000
DEFAULT_SENDERS = 10
DEFAULT_MEAN_WAIT = 0.1
DEFAULT_MEAN_FAILURE_RATE = 0.1
DEFAULT_MONITOR_INTERVAL = 1.0
DEFAULT_QUEUE_SIZE = 1000

# Biggest seed we'll invent when the user doesn't give us one. Any large number
# works; this is just a common choice (the largest 32-bit signed integer).
MAX_GENERATED_SEED = 2 ** 31 - 1


@dataclass(frozen=True)
class Config:
    """Every knob for one simulation run, in one object.

    frozen=True makes it read-only after creation, so nothing can quietly
    change a setting halfway through a run.
    """

    num_messages: int = DEFAULT_MESSAGES
    num_senders: int = DEFAULT_SENDERS
    mean_wait: float = DEFAULT_MEAN_WAIT
    mean_failure_rate: float = DEFAULT_MEAN_FAILURE_RATE
    monitor_interval: float = DEFAULT_MONITOR_INTERVAL
    queue_size: int = DEFAULT_QUEUE_SIZE
    seed: int = 0

    def __post_init__(self) -> None:
        """Runs automatically after the dataclass is built. Rejects bad values.

        Catching these here means the rest of the code can assume the numbers
        make sense. Without the mean_wait check, for example, a value of 0
        would blow up later with a confusing divide-by-zero inside the sender.
        """
        if self.num_messages < 0:
            raise ValueError("num_messages must not be negative")
        if self.num_senders < 1:
            raise ValueError("num_senders must be at least 1")
        if self.mean_wait <= 0:
            raise ValueError("mean_wait must be positive")
        if not 0.0 <= self.mean_failure_rate <= 1.0:
            raise ValueError("mean_failure_rate must be between 0 and 1")
        if self.monitor_interval <= 0:
            raise ValueError("monitor_interval must be positive")
        if self.queue_size < 1:
            raise ValueError("queue_size must be at least 1")


def derive_rng(seed: int, label: str) -> random.Random:
    """Build one independent random generator, named by `label`.

    This is the heart of reproducibility. Instead of one shared generator for
    the whole program, each part gets its own, built from the seed plus a label
    like "messages" or "sender:3".

    Two things come out of this:
      - Same seed and same label always produce the same sequence of numbers.
      - Different labels produce unrelated sequences, so senders running at the
        same time never interfere with each other's randomness.

    random.Random accepts a string as a seed, so "42:sender:3" works fine.
    """
    return random.Random("{}:{}".format(seed, label))


def parse_args(argv=None) -> Config:
    """Turn command-line arguments into a Config.

    If no seed was given we pick one at random. main.py prints it, so any run
    can be repeated later with --seed.
    """
    parser = argparse.ArgumentParser(
        prog="sms-sim",
        description="Simulate sending SMS alerts through a pool of concurrent senders.",
    )
    parser.add_argument(
        "-n", "--messages", type=int, default=DEFAULT_MESSAGES,
        help="number of messages to generate (default: %(default)s)",
    )
    parser.add_argument(
        "-s", "--senders", type=int, default=DEFAULT_SENDERS,
        help="number of concurrent senders (default: %(default)s)",
    )
    parser.add_argument(
        "-w", "--wait", type=float, default=DEFAULT_MEAN_WAIT,
        help="mean send wait time in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "-f", "--failure-rate", type=float, default=DEFAULT_MEAN_FAILURE_RATE,
        help="mean failure rate across senders, 0 to 1 (default: %(default)s)",
    )
    parser.add_argument(
        "-i", "--interval", type=float, default=DEFAULT_MONITOR_INTERVAL,
        help="seconds between progress reports (default: %(default)s)",
    )
    parser.add_argument(
        "-q", "--queue-size", type=int, default=DEFAULT_QUEUE_SIZE,
        help="max in-flight messages before the producer blocks (default: %(default)s)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="seed for reproducible runs (default: randomly generated and printed)",
    )

    args = parser.parse_args(argv)

    # `is not None` rather than a plain truth check, so --seed 0 still works.
    seed = args.seed if args.seed is not None else random.randrange(MAX_GENERATED_SEED)

    return Config(
        num_messages=args.messages,
        num_senders=args.senders,
        mean_wait=args.wait,
        mean_failure_rate=args.failure_rate,
        monitor_interval=args.interval,
        queue_size=args.queue_size,
        seed=seed,
    )
