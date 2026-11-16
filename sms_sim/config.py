"""Configuration, command-line parsing, and seed derivation.

Config owns the master seed, so it also owns the rule for splitting that seed
into independent child streams. Every source of randomness in the simulation
comes from `derive_rng`, which is what makes a run reproducible.
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass

DEFAULT_MESSAGES = 1000
DEFAULT_SENDERS = 10
DEFAULT_MEAN_WAIT = 0.1
DEFAULT_MEAN_FAILURE_RATE = 0.1
DEFAULT_MONITOR_INTERVAL = 1.0
DEFAULT_QUEUE_SIZE = 1000

# Upper bound for a randomly generated seed when the user does not supply one.
MAX_GENERATED_SEED = 2 ** 31 - 1


@dataclass(frozen=True)
class Config:
    """All tunable parameters for one simulation run."""

    num_messages: int = DEFAULT_MESSAGES
    num_senders: int = DEFAULT_SENDERS
    mean_wait: float = DEFAULT_MEAN_WAIT
    mean_failure_rate: float = DEFAULT_MEAN_FAILURE_RATE
    monitor_interval: float = DEFAULT_MONITOR_INTERVAL
    queue_size: int = DEFAULT_QUEUE_SIZE
    seed: int = 0

    def __post_init__(self) -> None:
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
    """Return an independent generator for one named stream of the run.

    Each caller gets its own `random.Random` instance, so draws never
    interleave between concurrent senders. The same (seed, label) pair always
    produces the same sequence, which is the whole basis of reproducibility.
    """
    return random.Random("{}:{}".format(seed, label))


def parse_args(argv=None) -> Config:
    """Build a Config from command-line arguments, generating a seed if absent."""
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
