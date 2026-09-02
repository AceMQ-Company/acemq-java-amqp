# Topology

```java
Topology orders = Topology.define()
        .exchange("orders", "topic")
        .queue("orders.new")
        .bind("orders.new", "orders", "order.*")
        .build();

mq.topology().apply(orders, ApplyMode.CREATE_ONLY);
```

## Why a plan rather than a declare

Declaring a topology at start-up is what almost everyone does, and it has one
specific failure mode. **AMQP forbids changing most queue settings in place.** A
queue that already exists with a different `x-message-ttl`, a different
dead-letter exchange, or created as classic where the code now asks for quorum
does not adapt: the declare is refused, and on AMQP 0-9-1 the refusal closes the
channel it happened on.

So the failure lands in production, at start-up, on the day of the deployment —
and the difference that caused it was visible in the diff all along.

Planning first moves that discovery earlier.

## The three modes

| | |
|---|---|
| `DRY_RUN` | Work it out, change nothing. What a build should run in review |
| `CREATE_ONLY` | Create what is missing, leave the rest. **The default** |
| `VALIDATE` | Change nothing, fail if anything is missing. For estates where topology is provisioned separately |

`CREATE_ONLY` is deliberately not destructive. Because a queue's arguments cannot
be edited, "changing" one is really a delete and a recreate, which discards its
messages. That is a migration, and a migration should be a decision rather than a
side effect of a process starting.

## Drift

A queue that exists but does not match is reported as **drift**, not as present:

```
topology plan:
  create   exchange orders (topic)
  DRIFT    queue orders.new (classic) — inequivalent arg 'x-message-ttl' for queue
           'orders.new' in vhost '/': received the value '30000' but current is the
           value '60000'
  create   binding orders.new <- orders [order.*]
```

```java
TopologyPlan plan = mq.topology().plan(orders);
if (plan.hasDrift()) {
    log.error("topology drift:\n{}", plan.render());
}
```

`apply` refuses when there is drift, before declaring anything at all. Without
that it would fail partway through, having created some of the topology, with a
channel-level protocol error instead of a message naming the argument.

`DRY_RUN` reports drift and does not throw — a dry run is how somebody finds out,
and throwing there would mean the only way to see the report is to trigger the
failure it warns about.

### How it is detected

AMQP has no way to read a queue's arguments back. Only the management HTTP API
can, and requiring that would mean a second endpoint, a second set of
credentials, and a check that does not work on a locked-down broker.

So the question is asked the other way round: the declaration is offered to the
broker on a **channel of its own**, and the refusal is read. RabbitMQ's 406 names
the argument and both values, which is more than an inspection API would have
given. The channel dies; nothing else does.

The queue is asked about passively first, so a plan never creates the topology it
was only supposed to report on.

### Resolving it

The library does not migrate queues, and that is deliberate. The safe order
depends on whether the queue can be drained first, whether consumers can be
stopped, and whether messages in it can be lost — none of which a library can
decide. What it will do is refuse to make things worse, and name the argument so
the decision is an informed one.

Two ways out, both yours:

- **Change the topology to match the broker.** Correct when the broker is right
  and the code drifted.
- **Migrate the queue** — create the replacement, move the messages, swap the
  bindings, delete the old one. Correct when the code is right.

### Transports that cannot tell

A transport with no way to inspect a queue reports `UNKNOWN` rather than
`PRESENT`. An unanswered question recorded as agreement is exactly how drift goes
unnoticed until a deployment, so the plan says which one it is.

The in-memory broker in `acemq-amqp-test` implements drift detection too, so a
topology change is caught by a unit test in milliseconds rather than only against
a container.

## What is not checked

**Exchanges and bindings are always reported as creations.** An exchange cannot
be inspected without a passive declare that fails the channel when it is absent,
and unlike a queue it holds no messages — redeclaring an equivalent one is
harmless. Bindings are idempotent for the same reason. Only queues hold messages,
so only queues are worth the round trip.

## Related

- [Reliability](reliability.html) — the retry ladder, which is topology the
  library generates for you
- [Getting started](getting-started.html)
