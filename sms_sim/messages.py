"""Making the random SMS messages.

The key choice here is that messages are produced one at a time (a generator)
rather than all at once into a list. With a list, a run of 10 million messages
would build 10 million strings in memory before sending even one. With a
generator, only the messages currently in the queue exist at any moment.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass
from typing import Iterator

# The spec says "up to 100 random characters".
MAX_BODY_LENGTH = 100

# Characters a message body can contain: letters, digits, and a little
# punctuation so the output looks vaguely like real text.
ALPHABET = string.ascii_letters + string.digits + " .,!?"


@dataclass(frozen=True)
class Message:
    """One SMS alert waiting to be sent.

    The id is just its position in the run (0, 1, 2, ...). It's useful for
    tracing a specific message and for debugging.
    """

    id: int
    body: str


def generate_messages(count: int, rng: random.Random) -> Iterator[Message]:
    """Produce `count` messages, one at a time.

    `yield` instead of `return` makes this a generator: nothing is computed
    until someone asks for the next message, and old messages are thrown away
    once used.

    Every random number comes from the single `rng` passed in, used in a fixed
    order, so the same generator always produces the same list of messages.
    """
    for message_id in range(count):
        # Length is random, between 1 and 100 characters.
        length = rng.randint(1, MAX_BODY_LENGTH)

        # choices() picks `length` characters at random (with repeats allowed),
        # and join() glues them into one string.
        body = "".join(rng.choices(ALPHABET, k=length))

        yield Message(id=message_id, body=body)
