# Patterns

```xml
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-patterns</artifactId>
  <version>0.2.8</version>
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

**Not here: saga, claim-check, scheduling.** Earlier versions of this
documentation listed all three. They were never built, and saying otherwise was
worse than saying nothing — a missing feature you know about is a decision, and
one you find out about at integration time is an outage. They are on the roadmap;
they are not in the jar.

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
