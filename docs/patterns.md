# Patterns

```xml
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-patterns</artifactId>
  <version>0.2.10</version>
</dependency>
```

Three things live here, and they are the three that every message-driven service
rebuilds badly: an idempotency store, a transactional outbox, and a schema
registry that survives a restart.

## What is here, and what is not

| | |
|---|---|
| **Idempotency store** | `InMemoryIdempotencyStore`, `JdbcIdempotencyStore` |
| **Transactional outbox** | `JdbcOutboxStore`, `OutboxRelay` |
| **Schema registry** | `JdbcSchemaRegistry` |
| **Claim check** | `ClaimCheckCodec`, `InMemoryClaimCheckStore`, `FilesystemClaimCheckStore` |
| **Scheduling** | `Scheduler` |
| **Saga** | `Saga`, `SagaResult` |

The last three were advertised here for months before they existed. That was
removed rather than excused, and they are now in the jar — built after three
applications had needed them, which is why the shapes are what they are.

Three more live in the core rather than here, because they are wired into the
consumer runtime: the [retry ladder](reliability.html),
[request/reply](request-reply.html), and pipelines — below.

## Pipelines

A flow with named stages, where each stage is its own queue and its own
consumer group:

```java
try (Pipeline<Order> fulfilment = mq.pipeline("fulfilment", Order.class)
        .step("validate", Order.class, m -> validate(m.payload()))
        .step("reserve", Reservation.class, m -> stock.reserve(m.payload()))
                .describedAs("hold stock for 15 minutes so payment cannot oversell")
                .withRetry(RetryPolicy.exponential(4, Duration.ofSeconds(1), Duration.ofMinutes(1)))
                .concurrency(4)
        .step("dispatch", Void.class, m -> shipping.dispatch(m.payload()))
        .build()) {

    fulfilment.send(order);
}
```

Each step gets a queue named `<pipeline>.<step>`, so a stage that is slow shows
up as a queue that is deep — you can see which stage, scale that one alone, and
retry it on its own schedule. A single consumer doing all four in sequence gives
you none of that.

The type flows through the builder: `reserve` receives what `validate` returned,
checked at compile time. A step returning `null` stops the run early, which is a
decision rather than a failure and is counted separately.

### Naming and describing a step

```java
.step("reserve", Reservation.class, handler)
        .describedAs("hold stock for 15 minutes so payment cannot oversell")
```

**The name is wire format.** It is the routing key, the queue suffix, and the
entry in the routing slip that tracks where a message has got to — so it is
restricted to letters, digits, dashes and dots, and renaming a step strands every
message currently in flight against the old name.

**The description is for people.** Free text, no constraints, no effect on
routing. It appears in the declaration log line and wherever a step is reported:

```
pipeline fulfilment declared with 3 steps: validate | reserve (hold stock for 15
minutes so payment cannot oversell) | dispatch
```

Keeping them separate is the point. A step name good enough to route on is rarely
a sentence, and a sentence is never safe to route on — so improving how a stage
reads should not be able to change where its messages go.

## Idempotency

At-least-once delivery means duplicates. An idempotency store is what turns
"delivered at least once" into "processed exactly once", and it is the half of
that guarantee the broker cannot provide.

```java
IdempotencyStore seen = new JdbcIdempotencyStore(dataSource);

mq.consume("payments.new", Payment.class,
        ConsumerOptions.prefetch(10).idempotent(seen),
        message -> payments.charge(message.payload()));
```

Keyed on the message id, which AceMQ sets on publish and which survives the retry
ladder unchanged.

### Claim, then work, then confirm

```java
store.claim(id);      // before the handler. false means somebody has it
store.confirm(id);    // after it returns
store.release(id);    // after it throws
```

**The claim is taken before the work, not after.** Recording afterwards leaves a
window — the process dies between the charge and the record — in which the work
happened and nothing knows, so the retry does it again. Closing that window is
the entire purpose, so the record has to come first.

The cost is the opposite failure: a process that dies after claiming and before
working leaves a claim blocking a retry that should happen. Hence the **lease** —
a claim not confirmed within `claimTimeout` expires and the message can be tried
again. Set it comfortably above your slowest handler.

| | |
|---|---|
| `InMemoryIdempotencyStore` | One process, bounded, evicts by age and size. Tests, and single-instance services that can tolerate forgetting on restart |
| `JdbcIdempotencyStore` | A table. Survives restarts, shared between replicas, and the uniqueness is the database's problem |

Two replicas racing on the same id: one insert wins, the other takes a constraint
violation and is told `false`. That is the whole of the distributed correctness,
and it is correct because databases are good at exactly this.

`purgeExpired()` on a schedule. Retention should exceed your longest retry
ladder by a comfortable margin.

## The transactional outbox

The problem it solves is not duplicates. It is **silence**:

```java
// Do not do this.
@Transactional
public void placeOrder(Order order) {
    orders.save(order);
    mq.publisher("orders", "order.placed").send(order);
}
```

The transaction covers the database and not the broker. When the save commits and
the publish fails, the order exists and nobody was told — and nothing anywhere
logs that as a problem, because both halves did what they were asked.

The fix is to write the message to the same database in the same transaction, so
one commit decides both:

```java
JdbcOutboxStore outbox = new JdbcOutboxStore(
        () -> currentTransactionConnection(),
        relayDataSource);

connection.setAutoCommit(false);
try {
    orders.save(connection, order);
    outbox.add(OutboxRecord.of("orders", "order.placed", envelope, json));
    connection.commit();
} catch (Exception e) {
    connection.rollback();
}
```

**Both writes must be on the same connection.** That is why the write side takes
a `ConnectionSupplier` and not a `DataSource`: a record written on a connection
from a pool is a record in a different transaction, which reproduces the exact
bug this pattern exists to prevent. The relay gets its own `DataSource` because
it runs outside your transaction and must not borrow it.

```java
try (OutboxRelay relay = new OutboxRelay(mq, outbox)) {
    relay.start();
}
```

The relay polls, claims a batch under a lease, publishes, and marks each row
published. A relay that dies mid-batch loses the lease and another takes it.

### What it does and does not promise

It gives you **at-least-once publishing, atomic with the database write**. The
message is never lost and is never sent for work that rolled back.

It does not give exactly-once publishing: the relay can publish and die before
recording that it did, and the next relay publishes again. The duplicate is
handled on the consuming side by the idempotency store. The pair is what people
mean by "exactly once"; neither half is sufficient alone.

### The relay publishes bytes unchanged

The record holds an already-serialised payload, and the relay republishes those
bytes exactly. This is not an implementation detail — a relay that deserialised
and re-encoded would double-encode the payload, which shipped once and is why
there is a test for it.

## The schema registry

Avro and Protobuf carry no description of themselves, so a reader must already
hold the schema the writer used. What travels in the message is a small integer,
and something must remember which integer means which schema.

```java
JdbcSchemaRegistry registry = new JdbcSchemaRegistry(dataSource);
registry.createSchemaIfAbsent();

mq.publisher("events", "order.placed", OrderPlaced.class)
        .as(AvroCodec.registered(registry));
```

**Identifiers must be stable forever.** A registry that hands out fresh ones on
restart makes every message written before the restart unreadable — and the bytes
still parse, just as the wrong schema, which is the worst way to fail.

`InMemorySchemaRegistry` (in the Avro module) forgets on restart and belongs in
tests only. Full detail is in [serialization](serialization.html).

## The claim check

```java
Codec checked = ClaimCheckCodec.wrapping(new JsonCodec(), store);
```

A scanned medical report is tens of megabytes. Putting it on a queue is possible
and is a mistake: it fills the broker's memory, it is copied to every bound
queue, it makes a dead-letter queue impossible to inspect, and it turns a broker
into a filesystem with worse tools. What travels instead is a **claim check** —
the payload goes to a store, the message carries the key.

**Only above a threshold.** Below 64 KiB by default the payload travels inline,
because offloading a small message turns one round trip into two — making the
common case slower to fix the rare one. The framing says which it is, so a
consumer reads both without being told, and the threshold can change without a
flag day. Messages written before the codec was introduced are still readable.

`ClaimCheckCodec.keyOf(body)` reads which object a message needs without
fetching it: the question in front of a dead-letter queue.

**Retention is what goes wrong.** The store and the queue have different
lifetimes and nothing relates them. A message replayed a month later carries a
key, and if the store expired it the replay produces a message nobody can read —
worse than a lost message, because it looks like a message. Store retention must
outlast every queue TTL, every dead-letter queue, and any manual replay.

## Scheduling

```java
scheduler.in(Duration.ofHours(4), "billing", "invoice.due", invoice);
scheduler.at(renewalDate, "policies", "policy.renew", policy);
```

**Not a per-message time to live.** The obvious implementation — set
`expiration`, drop it in a queue nobody consumes, let it dead-letter — is wrong
for anything but a single fixed delay, because a classic queue expires messages
**only at its head**. A four-hour message followed by a one-minute message
delivers the second one in four hours, and nothing reports it.

Instead: a ladder of queues each with a *uniform* TTL, and a message hops through
them until due. Every message in a rung has the same delay, so the head is always
the one due soonest. A four-hour delay is four one-hour hops.

The cost, stated plainly: a long delay is several round trips rather than one,
and accuracy is about the smallest rung. A scheduler that must fire at
09:00:00.000 is a scheduler, not a message broker.

## Saga

```java
Saga<Order> booking = Saga.<Order>named("place-order")
        .step("take-payment", order -> payments.charge(order))
                .compensateWith(order -> payments.refund(order))
        .step("reserve-stock", order -> inventory.reserve(order))
                .compensateWith(order -> inventory.release(order))
        .step("book-courier", order -> couriers.book(order))
        .build();

SagaResult result = booking.run(order);
```

If `book-courier` throws, the stock is released and the payment refunded, in that
order — reverse of the order the world was changed in.

**Not a distributed transaction, and the difference matters.** After
`take-payment` the money really has moved and anybody looking sees it. The refund
is a *new fact*, not an erasure, and for a few seconds the world contained a
charge that should not have happened. A saga is honest about that where a
two-phase commit pretends otherwise — so steps must be undoable *by doing
something else*. Sending an email cannot be compensated; the apology is a second
email. Put it last, after everything that can still fail.

**Not durable.** State is on the stack, so a crash midway leaves the saga
half-applied with nothing to resume it. Where that matters the steps have to be
messages and the state has to be in a database, which is a much larger thing.

### The number to alert on

```java
result.unresolved()   // steps whose compensation itself failed
```

When a compensation throws, it is logged and the remaining ones still run —
stopping would leave more undone than continuing. What comes back is the list of
effects that happened, were meant to be undone, and were not. **No retry resolves
those; a person does.**

## `createSchemaIfAbsent()` is for development

All three JDBC classes have it, and none of them should be running it in
production. The table belongs in whatever migration tool already owns your
schema; a library that creates tables at start-up is a library deciding when your
database changes.

## Related

- [Reliability](reliability.html) — retries and dead letters, the patterns that
  live in the core
- [Serialization](serialization.html) — the schema registry in context
- [Tutorial 3](tutorial-exactly-once.html) — all of this built up step by step
