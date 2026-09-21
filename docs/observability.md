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

`acemq-amqp-actuator` below is not a counter-example. It **records nothing**: the
numbers are still produced by the core, and that module only serves them over
HTTP for an application that has no other way to.

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

## Getting the numbers out of the process

Recording metrics and **serving** them are two different jobs, and only the first
one is in the core. Micrometer is a recording API: it needs a registry to record
into and something to serve that registry over HTTP, and neither is a thing a
messaging library should assume.

### With Spring Boot — use Actuator, not this

Add `spring-boot-starter-actuator` and `micrometer-registry-prometheus`, and
scrape `/actuator/prometheus`. Boot supplies the registry, the endpoint, the
security around it and the configuration for all three; the starter wires this
library into the same registry, so AceMQ's series appear alongside the JVM's and
the HTTP server's with nothing further to do. Quarkus, Micronaut, Helidon and any
servlet application are the same story with different names.

**This is the right answer whenever it is available.** Everything below is for
the applications where it is not.

### Without a framework — `acemq-amqp-actuator`

```xml
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-actuator</artifactId>
</dependency>
```

```java
try (AceMq mq = AceMq.connect("amqp://localhost");
     AceMqActuator actuator = AceMqActuator.start(mq)) {
    // http://127.0.7.3:9464/acemq-metrics
}
```

A worker, a batch consumer, a daemon or a command-line tool has no registry and
no HTTP server, and until this module existed the JVM was the only one of the
five libraries where such an application had to write an endpoint itself. Go
ships `actuator`, .NET ships `AceMq.Amqp.Diagnostics`, Python ships `prometheus`,
Ruby ships `telemetry`.

Three paths, the same three in every language, so one scrape configuration and
one probe serve a whole estate:

| | |
|---|---|
| `/acemq-metrics` | Prometheus text format, rendered by Micrometer |
| `/acemq-health` | JSON. **503** when the connection is not open |
| `/acemq-info` | JSON: library version, application version, transport capabilities |

Port **9464** by default, the OpenTelemetry convention, bound to **loopback**.

`AceMqActuator.start(mq)` needs no other wiring: `connect` with no telemetry
argument records into Micrometer's global registry, and the actuator attaches its
own registry to that. **Start it before the traffic.** Micrometer replays the
meters that already exist into a registry added later but not their accumulated
values, so anything published beforehand is counted nowhere the endpoint can see
it — the series appears, the scrape parses, and the number is simply low. Where
the ordering cannot be arranged, wire it explicitly:

```java
PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
AceMq mq = AceMq.connect(url, MicrometerSupport.telemetry(registry, "rabbitmq"));
AceMqActuator actuator = AceMqActuator.start(
        ActuatorOptions.builder().connection(mq).registry(registry).build());
```

#### What it costs, and what it does not do

The HTTP server is `com.sun.net.httpserver.HttpServer` from the JDK, so the
module adds no server dependency at all. It does add
`micrometer-registry-prometheus`, which is six Prometheus jars: that is the
largest cost here and it is deliberate. The alternative is writing the exposition
format by hand — name sanitisation, label escaping, the `_total` suffix, the
`+Inf` bucket — which is Micrometer's job and which it already does exactly as
every Spring Boot service in the estate does it, so the bytes match what the
dashboards expect.

**The endpoints are unauthenticated.** They name queues, report broker state and
show traffic rates. Loopback is the default for that reason: let the scraper
reach it from the same host, pod or sidecar, and where it genuinely has to be
reachable from elsewhere, put something in front that authenticates. The library
will not, and says so rather than shipping a token check nobody configures.

An application that already runs an HTTP server should not start a second one.
`actuator.metrics()` returns exactly what a scrape would receive, and
`actuator.registry()` hands over the registry itself — either can be served from
whatever endpoint that application already has.

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
| `acemq.messages.dead.lettered.total` | `outcome`: `dead_lettered`, `parked` |
| `acemq.messages.set.aside.failed` | `queue`, `target` — the copy never arrived |
| `acemq.retry.rung.missing` | `queue`, `rung` — a long backoff waited in the consumer instead |

Tags: `exchange`, `routing.key`, `queue`, `transport`, `message.type`, `outcome`,
`target`, `rung`.

### `rejected` and `dead_lettered` are different events

Both end in the dead-letter queue, and only the word keeps them apart:

- **`rejected`** — a handler gave up on this message by name, by throwing
  `AceFatalException`. A decision somebody's code took. The retry ladder is
  skipped entirely.
- **`dead_lettered`** — the engine gave up: the attempts ran out, or the message
  aged past the policy's limit. Nobody decided anything.

The distinction is the useful one. A rise in `rejected` points at a producer
sending something a consumer will never accept; a rise in `dead_lettered` points
at a dependency that is down. Java used to report both as `dead_lettered`, which
collapsed the two on every dashboard. It no longer does, so an alert or a
dashboard panel matching `outcome="dead_lettered"` to catch fatal handler
failures needs `outcome="rejected"` as well.

`acemq.messages.dead.lettered.total` counts both, because it is about where the
message went rather than why.

### `parked` and `dead_lettered` are different events

Where it went is the split `acemq.messages.dead.lettered.total` carries on its
`outcome` tag, and there are two destinations:

- **`dead_lettered`** — a handler ran and failed as many times as the policy
  allows, or the message aged out. It goes to `{queue}.dlq`.
- **`parked`** — nothing could decode the payload, so no handler ever ran. It
  goes to `{queue}.parked`, because a payload that will not parse now will not
  parse on any future attempt and retrying it only burns capacity.

The two want different responses. Dead-lettered is usually a dependency that is
down and will come back; parked is a deploy or a schema change and will not fix
itself. Go, .NET, Python and Ruby all have the same word for it.

An alert or a dashboard panel written against
`acemq.messages.dead.lettered.total` with no `outcome` selector still sees both
and is unaffected.

### When the message cannot even be set aside

`acemq.messages.set.aside.failed{queue,target}` counts the republish to
`{queue}.dlq` or `{queue}.parked` failing — almost always a queue that was never
declared. The message is settled back to the broker, which is the last thing
between it and nothing.

Worth an alert at any value above zero. It does not rise under load or during a
deploy; it rises when a topology is wrong. Nothing else in the estate
distinguishes it: a dead-letter queue that is missing and a dead-letter queue
doing its job both look like one queue draining.

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

### Every span carries an outcome

`messaging.acemq.outcome` is on every span this library ends, whatever happened,
because the counter for the same operation always carries an `outcome` tag. An
operation that threw is `failed`; one that closed without saying how it went is
read the same way, which is what it almost always is. That symmetry is the point:
a span with no outcome where the counter said `failed` means a dashboard shows
failures and a trace search for `messaging.acemq.outcome = "failed"` finds none
of the spans behind them.

An outcome named explicitly wins over the default. A request that timed out
reports `timed_out` even though the caller then unwound through an exception,
because `timed_out` says more than "it threw".

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

- [Tutorial 4](tutorial-observability.md) — the same ground, built up, with the
  procedure for reading a trace when something is wrong
- [Reliability](reliability.md) — what the retry and dead-letter numbers mean
