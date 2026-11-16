"""Message generation.

Messages are produced lazily by a generator rather than returned as a list, so
memory stays flat whether the run is 1,000 messages or 10,000,000.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass
from typing import Iterator

MAX_BODY_LENGTH = 100
ALPHABET = string.ascii_letters + string.digits + " .,!?"


@dataclass(frozen=True)
class Message:
    """One SMS alert waiting to be sent."""

    id: int
    body: str


def generate_messages(count: int, rng: random.Random) -> Iterator[Message]:
    """Yield `count` messages of up to MAX_BODY_LENGTH random characters.

    All draws come from the single `rng` passed in, in order, so the same
    generator state always yields the same messages.
    """
    for message_id in range(count):
        length = rng.randint(1, MAX_BODY_LENGTH)
        body = "".join(rng.choices(ALPHABET, k=length))
        yield Message(id=message_id, body=body)
