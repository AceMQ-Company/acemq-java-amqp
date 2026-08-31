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

Throw, and the retry ladder takes over — see [Reliability](reliability.html):

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

## Counting what happened

```java
consumer.acknowledged();
consumer.rejected();
consumer.retried();
consumer.deadLettered();
consumer.duplicates();     // suppressed by an idempotency store
consumer.inFlight();
```
