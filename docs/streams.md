# Streams

A stream is an append-only log the broker keeps until retention removes it.
Reading one does not empty it: every consumer holds its own position, and the
same message can be read again tomorrow, by someone else, from the beginning.

That sounds like a queue with better retention. It is not, and the differences
are silent rather than loud.

```java
mq.declareStream("orders.log", Duration.ofDays(7), 10L * 1024 * 1024 * 1024);

mq.stream("orders.log", OrderPlaced.class)
        .fromFirst()
        .consume(message -> projection.apply(message.payload()));
```

Both retention limits may be `null`, which is legal, logs a warning, and is
almost always a mistake — a stream with no limit grows until the disk is full,
and a full disk is a broker-wide alarm that blocks every publisher on the node.

## Where to start reading

```java
reader.fromFirst();                          // everything still held
reader.fromLast();                           // the last message, then onwards
reader.fromNext();                           // only what arrives from now on
reader.fromOffset(41_337);                   // an exact position
reader.from(Instant.now().minus(1, HOURS));  // by wall-clock time
reader.fromLast(Duration.ofDays(3));
```

A reader never told where to start reads from `fromNext()`. That is right for a
new consumer joining a live system and wrong for a projection — one built without
`fromFirst()` silently skips its own history and looks healthy while being wrong.
State the position rather than inheriting it.

## Resuming

The broker does not remember your position. That is what makes streams cheap, and
it is what makes checkpointing your job:

```java
try (StreamConsumer consumer = mq.stream("orders.log", OrderPlaced.class)
        .fromOffset(checkpoints.load("projection-a") + 1)
        .consume(message -> projection.apply(message.payload()))) {

    consumer.lastHandledOffset()
            .ifPresent(offset -> checkpoints.save("projection-a", offset));
}
```

Resume from one more than `lastHandledOffset()`. Saving the checkpoint in the
same transaction as the projection's own writes is what makes the pair exactly
once; anywhere else is at-least-once, which is fine if the handler is idempotent.

## What a stream cannot do

A stream never removes a message, and nearly every failure-handling tool in this
library is built on moving one:

| Queue behaviour | On a stream |
|---|---|
| Retry ladder | **Does not apply** — nothing can be moved to a rung and back |
| Dead-letter queue | **None.** A failed message stays where it is |
| Parking lot | **None**, for the same reason |
| Requeue | **Nothing to put back** — acknowledging only advances your position |
| Selective reject | **No.** Positions move forward; they do not skip holes |
| Destructive read | **No.** Other consumers still see everything you consumed |

So a failing handler has two honest outcomes and you must pick:

```java
mq.stream("orders.log", T.class).fromFirst().consume(handler);                 // stop
mq.stream("orders.log", T.class).fromFirst().skipFailures().consume(handler);  // skip
```

Stopping is the default because it is recoverable — the message is still there
and a consumer restarted at the same offset sees it again after a fix.

`skipFailures()` deserves thought: **nothing else records the gap.**
`consumer.skipped()` is the only evidence, and no dead-letter queue holds a copy.
Alert on it or it is invisible.

## Prefetch

A stream consumer always has one — 100 unless you say otherwise, never zero.
RabbitMQ closes the channel for a stream consumer without a prefetch, because it
is the only backpressure a stream has.

```java
mq.stream("orders.log", OrderPlaced.class).fromFirst().prefetch(200).consume(handler);
```

## Requirements

Streams need RabbitMQ 3.9 or later over `amqp://`. Anything else refuses rather
than degrades — falling back to a classic queue would lose replay, retention and
every consumer's independent position, which are the three reasons to want one.

**The in-memory test kit does not implement streams**, so stream tests need a
real broker.

## When to use one

Use a stream when more than one consumer needs the same messages, when history
must be re-readable, or when a projection has to be rebuildable from scratch:
event sourcing, audit logs, analytics fan-out.

Stay with a queue for work that is done once and finished. Streams have no
dead-letter queue, no retry ladder and no competing-consumer semantics, and
rebuilding those on top of one is how a simple job becomes a distributed systems
project.
