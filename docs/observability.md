# Observability

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

That is the configuration. `AceMq.connect(...)` detects whichever of the two is
on the classpath and reports to it.

Both are **`<optional>`** in the core, so an application that wants neither pays
nothing — `Telemetry.NONE` is a singleton with empty methods — and one that wants
both configures nothing.

## Why it is in the core

There is no `acemq-amqp-observability` module and there should not be. Telemetry
has to be emitted from inside the publisher and the consumer runtime, which is
where the timing and the outcome are known; a separate module would either be
empty or force the core to depend on it. The vendor coupling that a module would
have isolated is already isolated, by the optional dependencies and by
`MicrometerSupport` and `OpenTelemetrySupport` — two classes that exist purely so
no optional type is ever named in a signature the JVM would have to resolve.

## Explicit wiring

```java
MeterRegistry registry = new SimpleMeterRegistry();
Telemetry telemetry = MicrometerSupport.telemetry(registry, "rabbitmq");

try (AceMq mq = AceMq.connect("amqp://localhost", telemetry)) {
```

| | |
|---|---|
| `Telemetries.autoDetect(transport)` | What `connect` does by default |
| `Telemetries.composite(a, b)` | Report to several sinks |
| `MicrometerSupport.telemetry(registry, transport)` | A registry that is not the global one |
| `OpenTelemetrySupport.telemetry(otel, transport)` | Likewise for tracing |
| `Telemetry.NONE` | Off. What the test suite uses |

## Traces cross the broker

The publish span writes a **W3C `traceparent`** into the message headers, and the
consumer starts its span as a child of it. One trace covers both services and the
queue between them, with the queue wait visible as the gap.

`traceparent` rather than an `x-acemq-*` header, deliberately: a Python consumer
or an instrumented HTTP hop joins the same trace without knowing this library
exists.

Spans are named for the destination — `orders publish`, `orders.new process` —
because a span named after a method tells you which code ran and not which queue
it ran for.

| Attribute | |
|---|---|
| `messaging.system` | Transport short name |
| `messaging.destination.name` | Exchange on publish, queue on consume |
| `messaging.message.id` | Envelope id; the same key idempotency uses |
| `messaging.message.conversation_id` | Correlation id, constant across a flow |
| `messaging.rabbitmq.routing_key` | |
| `messaging.message.type` | |

`conversation_id` is what turns a trace into a story: everything caused by one
order carries it.

## Metrics

Names are fixed in `MetricNames` in the API rather than chosen per language, so
every port reports the same series and one dashboard serves all of them.

| | |
|---|---|
| `acemq.publish.duration` | Time to a confirm — not time to a socket write |
| `acemq.publish.total` | `outcome`: `confirmed`, `unroutable`, `failed` |
| `acemq.consume.duration` | Handler time |
| `acemq.consume.total` | `outcome`: `acked`, `retried`, `dead_lettered`, `rejected` |
| `acemq.consume.attempts` | Distribution of attempt numbers |
| `acemq.consume.in.flight` | Handlers running now |
| `acemq.messages.retried.total` | |
| `acemq.messages.dead.lettered.total` | |

Tags: `exchange`, `routing.key`, `queue`, `transport`, `message.type`, `outcome`.

### Alert on these four

**`acemq.publish.total{outcome="unroutable"} > 0`** — the broker accepted the
message and nothing was bound to receive it, so it is gone. Nothing throws,
because the broker did its job. Almost always a routing key typo or a binding
missing after a deploy.

**`acemq.messages.dead.lettered.total` rising** — something is failing
permanently.

**`acemq.consume.in.flight` pinned at your concurrency** — every handler is busy
and the queue is growing. This tells you to scale before depth does.

**`acemq.publish.duration` p99 climbing with flat throughput** — usually a broker
under memory pressure, about to block publishers.

### Do not alert on queue depth

A queue is a buffer; having things in it is the job. Depth that stops draining
matters, depth does not. Alert on the age of the oldest message, or on depth
rising for several minutes — a threshold on depth alone fires during every normal
burst and is muted within a week.

## The overhead is measured

Instrumentation nobody can afford gets disabled in production, which is where it
was needed. `TelemetryOverheadBenchmark` runs nightly against the real client: a
confirmed publish through AceMQ measures within noise of the same publish written
by hand, and the job fails if the gap exceeds five percent.

## Request/reply, the outbox and pipelines

These three counted their own work for a while and reported none of it — the
numbers existed, readable only by calling a getter on the object, which no
dashboard can do.

| | |
|---|---|
| `acemq.request.duration` | The round trip as the **caller** experienced it, tagged `answered` or `timed_out` |
| `acemq.request.total` | |
| `acemq.outbox.lag` | How long a record waited between being committed and being published |
| `acemq.outbox.total` | Tagged `published` or `failed` |
| `acemq.pipeline.run.duration` | How long a message had existed when it left a pipeline |
| `acemq.pipeline.run.total` | Tagged `pipeline`, `step` and `outcome`: `completed` or `ended_early` |

**`acemq.outbox.lag` is the one to alert on.** A committed, unpublished row is a
message that exists, is owed to somebody, and appears in **no queue depth
anywhere** — a stopped relay looks exactly like a system with nothing to send.
Nothing else in this list can see it.

`acemq.request.duration` exists because neither of the spans that already covered
a request/reply call was the thing the caller waited for. The publish is timed
and the reply's delivery is timed; "how long did asking take" was the gap between
them, and a gap is not a measurement. It traces as a `CLIENT` span with the
publish and the reply as its children.

Only the blocking `request(...)` is timed. `requestAsync` hands back a future
with no timeout attached, so the wait belongs to whoever holds it — a scope
closed at return would time the publish and label it the round trip.

### Adding your own sink

`Telemetry`'s newer methods are **`default` no-ops**, deliberately and
permanently. The interface is implemented by applications with their own
monitoring, and every abstract method added after the fact breaks all of them at
compile time for a signal they never asked for. Override what you want; a sink
written before a method existed keeps working. There is a test asserting exactly
that.

## What is still not instrumented

- **Pipeline steps have no span of their own.** They are legible — a step's queue
  is `<pipeline>.<step>`, so it traces as `orders.validate process` — but there is
  no span around the stage, so the run duration above is measured from the
  envelope's age rather than assembled from per-stage timings.
- **Consumer spans start after decoding.** A message that fails to decode is
  parked without a span, so the trace ends at the publish. Rare, and confusing
  when it happens.

## Related

- [Tutorial 4](tutorial-observability.html) — the same ground, built up, with the
  procedure for reading a trace when something is wrong
- [Reliability](reliability.html) — what the retry and dead-letter numbers mean
