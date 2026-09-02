# Tutorial 4 — Seeing what happens

**20 minutes.** Continues from [tutorial 3](tutorial-exactly-once.html).

A message goes in and does not come out. This tutorial is about answering *where
did it stop* in under a minute, rather than by grepping four services' logs for
an order id.

## Step 1 — Turn it on

```xml
<dependency>
  <groupId>io.micrometer</groupId>
  <artifactId>micrometer-core</artifactId>
</dependency>
<dependency>
  <groupId>io.opentelemetry</groupId>
  <artifactId>opentelemetry-api</artifactId>
</dependency>
```

That is the whole of it. There is no second step.

```java
try (AceMq mq = AceMq.connect("amqp://localhost")) {
```

`connect` calls `Telemetries.autoDetect(...)`, which looks for Micrometer and
OpenTelemetry on the classpath and reports to whichever it finds. Neither is a
required dependency — both are `<optional>` in the core — so an application that
wants neither pays nothing and one that wants both configures nothing.

To be explicit, or to point at a registry that is not the global one:

```java
MeterRegistry registry = new SimpleMeterRegistry();
Telemetry telemetry = MicrometerSupport.telemetry(registry, "rabbitmq");

try (AceMq mq = AceMq.connect("amqp://localhost", telemetry)) {
```

`Telemetries.composite(a, b)` combines sinks; `Telemetry.NONE` switches
everything off, which is what the tests in tutorials 1 and 2 were using.

## Step 2 — The trace crosses the broker

This is the part that matters and the part most setups do not have.

```java
// service A
mq.publisher("orders", "order.placed", Order.class).send(order);

// service B, a different process, possibly minutes later
mq.consume("orders.new", Order.class, message -> shipping.reserve(message.payload()));
```

The publish span writes a **W3C `traceparent`** into the message headers. The
consumer reads it back and starts its span as a child of the publish. One trace,
spanning both services and the queue between them, with the queue wait visible as
the gap.

Without this you have two unrelated traces and a human joining them by eye. With
it, "where did the order stop" is one query.

`traceparent` is the W3C standard header, not an `x-acemq-*` invention, so a
service that has never heard of this library — a Python consumer, an
OpenTelemetry-instrumented HTTP hop — joins the same trace.

**Spans are named after the destination**, so they read as `orders publish` and
`orders.new process` rather than as method names.

| Attribute | |
|---|---|
| `messaging.system` | The transport: `rabbitmq` |
| `messaging.destination.name` | Exchange when publishing, queue when consuming |
| `messaging.message.id` | The envelope id — the same one idempotency keys on |
| `messaging.message.conversation_id` | The correlation id, constant across a whole flow |

That last one is what turns a trace into a story: everything caused by one order
shares it.

## Step 3 — The metrics

Names are fixed in `MetricNames` rather than chosen per language, so a Go service
and a Java service report the same series and a dashboard works for both.

| | |
|---|---|
| `acemq.publish.duration` | How long a confirm took. **The publisher-side latency that matters** |
| `acemq.publish.total` | Tagged `outcome`: `confirmed`, `unroutable`, `failed` |
| `acemq.consume.duration` | Handler time |
| `acemq.consume.total` | Tagged `outcome`: `acked`, `retried`, `dead_lettered`, `rejected` |
| `acemq.consume.attempts` | The distribution of attempt numbers |
| `acemq.consume.in.flight` | Handlers running right now |
| `acemq.messages.retried.total` | |
| `acemq.messages.dead.lettered.total` | |

Tags: `exchange`, `routing.key`, `queue`, `transport`, `message.type`, `outcome`.

### The four to alert on

**`acemq.publish.total{outcome="unroutable"}` above zero.** The broker took the
message and nothing was bound to receive it. It is gone. This is almost always a
routing key typo or a missing binding after a deploy, and it is silent by
default — nothing throws, because the broker did accept it.

**`acemq.messages.dead.lettered.total` rising.** Something is failing
permanently. Pair it with the depth of the `.dlq` queue.

**`acemq.consume.in.flight` pinned at your concurrency.** Every handler is busy;
the queue is growing behind them. This is the number that tells you to scale
before the depth does.

**`acemq.publish.duration` p99 climbing** while throughput is flat. Usually the
broker under memory pressure, about to block publishers entirely.

### What not to alert on

Queue depth alone. A queue is a buffer and it is *supposed* to have things in it.
Depth that is not draining matters; depth does not. Alert on the age of the
oldest message, or on depth rising for N minutes — never on a threshold.

## Step 4 — Reading it when something is wrong

An order was placed and never shipped. In order:

1. **Find the trace** by order id — you set it as the correlation id, so it is an
   attribute on every span.
2. **Look at where it ends.** A publish span with no matching process span means
   it never got to the consumer: check `unroutable`.
3. **Two process spans** means it was retried. Check the error on the first.
4. **A process span with a long gap before it** means it queued. Not an error;
   check `in.flight` and scale.
5. **No spans at all** means it never got published. Check the outbox table from
   tutorial 3 — the row will be there, unpublished, with `lastError` set.

Step 5 is the one that catches people. If the outbox has it and the relay does
not, no amount of broker monitoring will show you anything, because as far as the
broker is concerned the message does not exist.

## Step 5 — The overhead

Instrumentation you cannot afford gets turned off in production, which is where
you need it. So it is measured rather than asserted: `TelemetryOverheadBenchmark`
runs nightly, and a confirmed publish through AceMQ measures within noise of the
same publish written by hand against the RabbitMQ client.

`Telemetry.NONE` is a singleton with empty methods, so an application without
either dependency does not pay for the branch either.

## Step 6 — The three that used to be invisible

Request/reply, the outbox relay and pipelines each counted their own work and
reported none of it. They do now:

| | |
|---|---|
| `acemq.request.duration` | The round trip as the caller experienced it |
| `acemq.outbox.lag` | How long a record waited between commit and publish |
| `acemq.pipeline.run.duration` | How long a message had existed when it left |

**`acemq.outbox.lag` is the one to put on a dashboard today.** The last item in
step 4 said a message stuck in the outbox is invisible to every broker metric —
this is the number that sees it. A relay that has died shows as lag climbing without
bound, and nothing else anywhere changes.

## What is still not instrumented

- **Pipeline steps have no span of their own.** They are legible as
  `orders.validate process`, but the run duration comes from the message's age
  rather than from per-stage timings.
- **Consumer spans start after decoding**, so a message that cannot be decoded is
  parked with no span and its trace ends at the publish.

## Next

You have finished the tutorials. From here:

- [Reliability](reliability.html) — the full retry, dead-letter and replay guide
- [Topology](topology.html) — declaring exchanges and queues, and catching drift
- The [examples repository](https://github.com/AceMQ-Company/acemq-java-amqp-examples)
  — 26 runnable programs and a complete microservice application
