# sms-sim

A simulation of sending SMS alerts through a pool of concurrent senders, with
live progress reporting and reproducible randomness.

The system generates a configurable number of random messages, feeds them
through a bounded queue to a pool of senders that each simulate network latency
and failures, and reports progress on a fixed interval.

## Quick start

Requires Python 3.9 or newer. No third-party dependencies are needed to run the
simulation; `pytest` is only needed for the tests.

```bash
python3 run.py
```

That runs the defaults: 1,000 messages, 10 senders, 0.1s mean send time, 10%
mean failure rate, progress every second.

To run the tests:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q
```

The suite runs in well under a second because the senders are driven by a fake
clock rather than actually waiting.

## Usage

```
python3 run.py [options]

  -n, --messages N      number of messages to generate      (default: 1000)
  -s, --senders N       number of concurrent senders        (default: 10)
  -w, --wait SECONDS    mean send wait time                 (default: 0.1)
  -f, --failure-rate R  mean failure rate across senders    (default: 0.1)
  -i, --interval SEC    seconds between progress reports    (default: 1.0)
  -q, --queue-size N    max in-flight messages              (default: 1000)
      --seed N          seed for reproducible runs          (default: random)
```

If no seed is given, one is generated and printed so the run can be replayed.

### Example

```
$ python3 run.py --seed 42
sms-sim: 1000 messages, 10 senders, mean wait 0.1s, mean failure rate 0.1
seed: 42  (re-run with --seed 42 to reproduce)
------------------------------------------------------------------------------
[    1.0s] sent=     86  failed=     15  avg=0.0853s  throughput=    100.4 msg/s
[    2.0s] sent=    193  failed=     23  avg=0.0885s  throughput=    107.7 msg/s
[    3.0s] sent=    287  failed=     36  avg=0.0896s  throughput=    107.5 msg/s
...
------------------------------------------------------------------------------
done in 9.57s
  processed:  1000
  sent:       894
  failed:     106
  avg send:   0.0933s
  throughput: 104.5 msg/s
```

Ten senders each taking 0.1s on average gives a theoretical ceiling of 100
messages/second, and the observed 104.5 msg/s matches that.

## Architecture

```
                   Config (n, senders, wait, failure rate, seed)
                                      |
                              derives seed streams
                                      |
   +----------------+        +--------v---------+        +---------------+
   |    Producer    |        |   Sender pool    |        |    Monitor    |
   |  streams n     |--queue-|  m senders, each |        |  prints every |
   |  messages      | bounded|  with own rng    |        |  n seconds    |
   +----------------+        +--------+---------+        +-------^-------+
                                      | records                  | reads
                                      |     +--------------------+
                                      +---->|  Stats (shared)    |
                                            +--------------------+
```

Four components, each in its own module:

- **Producer** (`main.run_producer`) streams messages into the queue one at a
  time, then puts one shutdown sentinel per sender so the pool drains and exits.
- **Queue** is an `asyncio.Queue` with a `maxsize`. When senders fall behind,
  the queue fills and `put` blocks, so the producer slows down instead of
  letting unsent messages pile up in memory.
- **Sender pool** (`sender.py`) is one coroutine per sender. Each pulls a
  message, draws a wait time, sleeps, rolls against its own failure rate, and
  records the result.
- **Monitor** (`stats.run_monitor`) wakes on an interval and prints. It only
  reads `Stats` and never takes part in sending, so changing the reporting
  interval cannot affect the throughput it reports.

### Module layout

```
sms_sim/
  config.py     Config dataclass, CLI parsing, seed derivation
  messages.py   Message type and the lazy message generator
  sender.py     Sender, failure-rate assignment, the sender loop
  stats.py      Stats totals and the periodic monitor
  main.py       Producer and wiring
tests/
  test_sms_sim.py
```

## Design decisions

### Why asyncio rather than threads

The simulated work is pure waiting — there is no CPU work anywhere in a send.
A sleeping coroutine costs a few hundred bytes, while a thread costs roughly
8 MB of stack to do exactly the same nothing. That difference is what makes
5,000 senders practical in a single process.

Running on one thread also removes a whole category of problems: coroutines
only switch at an `await`, so shared state cannot be corrupted mid-update.

The cost is a single-core ceiling, measured below.

### Why the Stats counter needs no lock

`Stats.record` contains no `await`. Because asyncio only switches tasks at an
`await` point, a `record` call always runs to completion before any other
sender gets a turn — two senders cannot interleave inside it. Under threads
this would need a lock; here it does not.

### Reproducible randomness

The requirement is that the same seed produces identical messages and identical
per-sender behaviour. The obvious implementation — one global generator shared
by every sender — does **not** satisfy it. Concurrent senders would draw from
that generator in whatever order the event loop happened to schedule them, so
the draws would differ between runs.

Instead, the master seed is split into independent named streams
(`config.derive_rng`):

```
master seed
  |- "messages"        -> generates all n message bodies
  |- "failure-rates"   -> draws m failure rates, in sender-index order
  |- "sender:0"        -> private to sender 0
  |- "sender:1"        -> private to sender 1
  \- ...
```

This gives three guarantees:

1. **Messages are identical** across runs, because they come from one stream
   consumed by one task in order.
2. **Sender i always has the same failure rate**, because all rates are drawn
   in index order from a dedicated stream before any sender starts.
3. **Sender i's k-th send always draws the same wait and the same failure
   roll**, because it draws only from its own stream, which nothing else
   touches.

What is deliberately *not* guaranteed is which sender handles which message —
that depends on scheduling, and the specification allows it to vary.

One invariant keeps this working: `Sender.send` consumes exactly two draws per
message, always in the same order (wait time, then failure roll). If a code
path ever consumed a different number of draws, the streams would drift out of
alignment and reproducibility would quietly degrade.

### Distribution choices

**Wait times** are drawn from an exponential distribution with the configured
mean. It is the standard model for service times, it is memoryless, and it
takes a single parameter.

**Failure rates** are drawn uniformly from `[0, 2 × mean]`, which has a mean of
exactly the configured value while giving each sender a different rate. A Beta
distribution with mean `p` would be more principled — it is bounded on `[0, 1]`
by construction and has a tunable spread — but uniform is simpler to reason
about and correct for the realistic range of failure rates. The clamp to
`[0, 1]` only matters when the mean exceeds 0.5, where it pulls the realised
mean slightly down.

**A failed send still costs time.** The sender waits first and then fails,
which is how a timeout behaves. This is also why the specification asks for an
average send time that includes failures.

### Testability

`Sender` takes its `sleep` and `clock` as constructor arguments, defaulting to
`asyncio.sleep` and `time.monotonic`, and `simulate()` passes them through.
Tests inject a fake clock that advances instantly, so a simulation of 1,000
messages at 0.05s each finishes in milliseconds instead of 5 seconds.

Because every test also fixes the seed, assertions are on exact values rather
than statistical ranges. A seeded test should never be flaky.

## Performance

Measured on an Apple Silicon Mac, Python 3.9, single process:

| Messages | Senders | Mean wait | Wall time | Throughput |
|---------:|--------:|----------:|----------:|-----------:|
|    1,000 |      10 |     0.1s  |    9.6s   |    105 msg/s |
|  200,000 |   2,000 |     0.01s |    1.9s   | 105,600 msg/s |
|  200,000 |   5,000 |    0.002s |    2.1s   |  95,400 msg/s |

The first row is latency-bound and matches the theoretical ceiling of
senders ÷ mean wait. The last two rows are the event loop itself saturating at
roughly 100,000 events/second — adding senders past that point does not help.

That saturation is visible in the metrics: in the third run the mean wait drawn
was 0.002s but the measured average send time was 0.049s. The extra 0.047s is
scheduler lag. Measuring wall-clock time around the send, rather than just
reporting the value the generator drew, is what makes that visible.

## Known limitations

- **Single core.** One event loop on one thread, so throughput tops out around
  100k messages/second regardless of sender count.
- **`asyncio.sleep` granularity** is roughly 1ms, so mean wait times below that
  are not simulated accurately.
- **In-memory queue.** Nothing is durable — if the process dies, in-flight
  messages are lost.
- **No retries or dead-letter handling.** A failed send is counted and
  discarded, which is what the specification asks for.

## Scaling further

Each limitation has a clear next step, in rough order of effort:

1. **Multiple processes.** Run one event loop per core with a shared queue.
   Senders would need per-process stat slots aggregated at the end, and `Stats`
   would need real synchronisation rather than relying on single-threaded
   semantics.
2. **An external broker.** Replacing `asyncio.Queue` with Redis or Kafka
   decouples the producer from the senders entirely and allows senders on
   different machines. Only the producer and the sender loop touch the queue,
   so this is a contained change.
3. **Pushed metrics.** With senders spread across machines, printing to stdout
   stops working. Each sender would export counters to something like
   Prometheus, with aggregation happening at the collector.
