# Consuming

```java
try (MessageConsumer consumer = mq.consume("orders.new", Order.class,
        message -> payments.charge(message.payload()))) {
    // runs until closed
}
```

The handler runs on threads the library owns. The message is acknowledged
**after** it returns — not when it was delivered — so a handler that throws, or a
process that dies mid-handler, does not lose the message.

## Prefetch

Prefetch is how many unacknowledged messages the broker will hand you at once,
and it is the only backpressure that exists. It defaults to 50.

```java
mq.consume("orders.new", Order.class, ConsumerOptions.prefetch(200), handler);
```

The number that matters is how much work is in flight when a consumer dies:
everything unacknowledged is redelivered. High prefetch with a slow handler means
a long stall after a crash and one consumer hogging a queue its peers could be
draining. Start low.

## Concurrency

One consumer is single-threaded per delivery. For more, run a group:

```java
ConsumerGroup group = mq.consumeGroup("orders.new", Order.class)
        .consumers(4)
        .prefetch(20)
        .handle(order -> payments.charge(order));

group.scaleTo(8);       // at runtime, no redeploy
group.prefetch(50);     // also at runtime
```

Scaling down drains: the consumers being removed stop taking new messages and
finish what they are holding.

The number you ask for is the number that runs. Handlers dispatch on a pool that
grows to fit the consumers on the connection, so `consumers(40)` waiting on a
payment gateway is forty handlers at once on a two-core pod, not two. This is
worth stating because the RabbitMQ Java client does the opposite left alone: its
default dispatch pool has one thread per core, shared by every consumer on the
connection, and the surplus consumers wait with nothing reporting that they are.

## Shutting down

Draining has a budget, and the budget is a **total** rather than an allowance per
consumer:

```java
group.drainTimeout(Duration.ofSeconds(20));   // the default

mq.close();                                   // spends 20 seconds in total
mq.close(Duration.ofSeconds(10));             // or whatever your grace period leaves
```

Twenty seconds because the number it has to fit inside is usually Kubernetes'
default `terminationGracePeriodSeconds` of 30. Handing the figure out again to
each consumer in turn would not be a budget at all: eight consumers at twenty
seconds each is nearly three minutes, the pod is killed at thirty seconds, and
everything still held is redelivered — which is the outcome draining exists to
avoid, reached slowly. One deadline is shared by every consumer in a group, and
by every group on a connection.

Consumers whose share of the deadline is already gone are still **stopped**; they
are simply not waited for. Skipping them entirely would leave consumers taking
new work while the rest of the application shut down around them.

## Ordering

Competing consumers process in parallel, which means out of order. When order
matters *per entity* — and it almost never matters globally — partition by key:

```java
mq.ordered("orders.new", Order.class)
  .partitions(8)
  .key(order -> order.customerId())
  .onFailure(OnFailure.STOP)
  .handle(order -> ledger.apply(order));
```

Every message for one customer lands in one partition and is handled in
sequence; different customers run in parallel. `partitions(8)` is a throughput
ceiling of eight, and changing it later reshuffles which key goes where.

`onFailure` is a real decision:

| | |
|---|---|
| `STOP` | that partition halts; the message stays. Nothing after it is processed. |
| `RETRY_IN_PLACE` | keep retrying the same message, blocking the partition |
| `SKIP` | move on, and record that a gap exists |

`STOP` is the default because a strict ordering guarantee you silently skip past
is not a guarantee.

## Failure

Throw, and the retry policy takes over — see [Reliability](reliability.md).
Short waits are held in the consumer; waits of thirty seconds or more are handed
to a rung queue in the broker:

```java
mq.consume("orders.new", Order.class,
        ConsumerOptions.prefetch(20).withRetry(RetryPolicy.exponential(5,
                Duration.ofSeconds(1), Duration.ofMinutes(5))),
        message -> payments.charge(message.payload()));
```

Two exceptions mean something specific:

- `AceFatalException` — retrying cannot help. Skips the ladder entirely and goes
  straight to the dead-letter queue. Throw it for validation failures and
  anything else a retry will hit identically.
- Anything else — transient. Retried on the schedule.

A payload that cannot be *decoded* never reaches your handler and is never
retried: it goes to the parking lot with its original bytes intact, because a
message that fails to parse will fail to parse on every attempt.

## Reading the envelope

```java
mq.consume("orders.new", Order.class, message -> {
    Envelope e = message.envelope();
    e.id();              // the message identifier
    e.correlationId();   // stitches a whole flow together
    e.attempt();         // which try this is
    e.replayCount();     // times it has been round the dead-letter loop
    e.error();           // why it was dead-lettered, if it was
    message.headers();   // your own headers; AceMQ's are on the envelope
});
```

`message.headers()` is your own headers only. Anything named `x-acemq-` is the
engine's and is stripped on the way in, which is why the fields above are read off
the envelope rather than out of the map — and why a header of yours must not use
that prefix. [The envelope](envelope.md) has the full header contract, the
defaults, and what the two AceMQ namespaces are for.

## Counting what happened

```java
consumer.acknowledged();
consumer.rejected();
consumer.retried();
consumer.deadLettered();
consumer.duplicates();     // suppressed by an idempotency store
consumer.inFlight();
```
