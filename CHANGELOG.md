# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added
- Maven multi-module skeleton with the six modules of the target architecture.
- Build quality gates: enforcer (Maven 3.8+, JDK 17+ toolchain), Spotless with
  the Palantir Java format and a license header, JaCoCo with a coverage gate,
  and reproducible flattened POMs.
- Java 11 bytecode target on a JDK 17 or newer toolchain, so Spring Boot 2.7
  applications on Java 11 can consume the library.
- First API types: `Capability`, `AceMqException`, `AceRetryableException`,
  `AceFatalException`.
- Continuous integration: build and test matrix, formatting check, and the
  authorship guard.
- Transport SPI: `Transport`, `TransportConnection`, `OutboundMessage`,
  `InboundDelivery`, `Acknowledger`, `Subscription` and `ConfirmResult`. The
  seam carries no AceMQ semantics, so one engine can serve AMQP 0-9-1 and
  AMQP 1.0.
- RabbitMQ transport over the RabbitMQ Java client, with automatic connection
  and topology recovery, a dedicated confirm-mode publishing channel, one
  channel per subscription, and unroutable-return correlation.
- Core engine: `AceMq` facade, publisher with awaited confirms, consumer runtime
  that settles every delivery exactly once, envelope-to-header mapping, and
  `ServiceLoader` transport discovery.
- A publish that the broker cannot route now raises `PublishFailedException`
  instead of being silently discarded.
- Quorum queues are the default for `declareQueue`, and are refused with a clear
  message when the broker does not support them.
- Verified end to end against a real RabbitMQ 4 container: envelope round trip,
  confirms, unroutable detection, handler failure, undecodable payload, fatal
  rejection and consumer shutdown.

- In-memory transport in `acemq-amqp-test`, selected by a `memory://` URL. It
  implements the same SPI as a real broker binding, so the whole engine can be
  exercised without Docker: exchange routing (direct, topic with correct `*` and
  `#` semantics, fanout), prefetch modelled as a settlement window, requeue on
  rejection, and refusal to redeclare an exchange with a different type. It
  claims only the capabilities it implements, so code depending on quorum
  queues, dead-lettering or delayed delivery fails against it exactly as it
  would against a broker that lacks them.

- `RetryPolicy`: fixed and exponential schedules with a multiplier, a ceiling,
  jitter on by default, and two independent give-up conditions — attempts used
  up, or the message grown older than a limit measured from its first publish.
- A retry ladder that waits inside the broker. Each distinct delay gets a queue
  with a time-to-live and a dead-letter target pointing back at the source, so a
  failed message waits without occupying a consumer. Enable it with
  `ConsumerOptions.withRetry(policy)`.
- Convention-based `{queue}.dlq` and `{queue}.parked` queues. Exhausted or
  over-age messages are dead-lettered with the reason attached; payloads that
  cannot be decoded are parked immediately, since they will never decode.
- `Envelope.error()` carries why a message was given up on, so a consumer of a
  dead-letter queue reads it through the API rather than by knowing a header
  name.
- `MessageConsumer.retried()` and `deadLettered()` counters.
- The in-memory transport implements queue time-to-live and dead-lettering, so
  retry behaviour is testable in milliseconds rather than only against a
  container.

- Cluster testing at 1, 3, 5 and 9 nodes. One and three nodes run in the
  ordinary build through a Testcontainers harness that joins real brokers into a
  cluster; five and nine run nightly from `compose/cluster-<size>.yml`, since
  that many brokers do not fit a per-test lifecycle on a hosted runner.
- Failover coverage that a single node cannot provide: a quorum queue keeps its
  messages when a node is lost, and publishing with confirms continues against a
  degraded cluster that still holds a majority.
- An assertion that a quorum queue is replicated to three nodes rather than to
  every node in the cluster, verified on real five and nine node clusters: on
  nine nodes each queue still has exactly three replicas, placed on different
  subsets of the cluster. Replicating everywhere still works, so no functional
  test would catch it; the cost appears only as a round trip per replica on every
  confirm. Verified against a real five-node cluster.

- Telemetry. Every publish and every delivery is timed and counted, and emits a
  span; retries and dead letters are counted and recorded as span events.
  `MetricNames` freezes the metric, tag and span names as public API, since a
  dashboard or an alert is written against those strings.
- Trace context travels with the message in the W3C `traceparent` header, so a
  handler's span is a child of the publish that caused it even across processes.
- `MicrometerSupport.telemetry(registry, transport)` and
  `OpenTelemetrySupport.telemetry(openTelemetry, transport)` take an explicit
  provider; `AceMq.connect(url, telemetry)` accepts one. Auto-detection remains
  as a fallback but reaches for process-wide global state, which two connections
  cannot share and a test cannot isolate.
- Telemetry is off by default in the sense that matters: with neither library on
  the classpath the engine uses a sink whose methods are empty.

- An aggregate coverage report in `acemq-amqp-coverage`, enforced at 83 % line
  and 60 % branch. The engine is exercised almost entirely from the test kit and
  the transport integration tests, so a per-module measurement reported nothing
  for the code that matters most.
- Per-module coverage gates where a module's own tests are a fair measure of it.
- `japicmp` wired for binary compatibility. It tolerates the absence of a
  previous version today and starts enforcing the moment `0.0.1` is released.

- JSpecify nullness annotations at `provided` scope, so they are visible while
  compiling and absent from a consumer's classpath. Every package is
  `@NullMarked`: non-null is the default and `@Nullable` is the exception. This
  is the annotation set Spring Framework 7 and Boot 4 adopted, which matters for
  a library whose main audience is Spring.
- Nullness checking is now an error rather than a warning: the codebase reports
  zero NullAway findings. Test sources are excluded, because several tests pass
  null deliberately to prove a guard throws.
- ErrorProne and NullAway, on JDK 21 through 23. ErrorProne compiles against
  javac internals and trails new releases, so the JDK 25 job builds the same code
  without it: unanalysed rather than unbuilt.

- `acemq-amqp-benchmarks`, a JMH module behind `-Pbenchmarks` so an ordinary
  build never waits for it (ADR-017). It measures what instrumentation costs,
  which doc 10 requires to be a published number rather than a promise.
- A nightly workflow that runs the benchmarks, compares each one against a
  stored baseline, keeps the JSON for ninety days and opens an issue on a
  regression.

- A topology planner. `Topology` declares what should exist as data;
  `mq.topology().plan(...)` works out what would change and renders it for a
  build log or a review; `apply(..., mode)` carries it out. `DRY_RUN` changes
  nothing, `CREATE_ONLY` creates what is missing and touches nothing else, and
  `VALIDATE` refuses to start when the topology is absent.
- `TransportConnection.queueExists` asks the broker what is there without
  creating it, implemented with a passive declare on RabbitMQ.

- An idempotent consumer. `ConsumerOptions.idempotent(store)` handles each
  message once however often the broker delivers it, which every broker worth
  using does at least twice sooner or later. `IdempotencyStore` claims, confirms
  on success and releases on failure, so a failed attempt can still be retried.
- `InMemoryIdempotencyStore`, bounded by both retention and size, and the first
  occupant of the previously empty patterns module.
- `MessageConsumer.duplicates()` counts deliveries recognised as already handled.
- A transactional outbox. `OutboxStore` writes a message in the caller's own
  transaction, so it becomes durable exactly when the work it announces does, and
  `OutboxRelay` publishes it afterwards on its own thread. `JdbcOutboxStore` keeps
  the outbox in a table beside the business data, claiming by lease so a relay that
  dies mid-batch strands nothing.
- The outbox and the idempotent consumer are two halves of one guarantee: the relay
  publishes with the record's own identifier, which is what lets the consumer
  recognise the copy a crashed relay sends again.
- `JsonCodec`, so a payload can be an object rather than a string. Named rather than
  detected: Jackson is on nearly every classpath by accident, and switching format
  on that basis would change a contract the application never asked to change.
  Unknown fields are ignored and dates are written as ISO-8601 text, both so that a
  message can outlive the code that wrote it.
- `CompositeCodec`, which writes one format and reads several, so changing format is
  two ordinary releases rather than a flag day.
- `Codec.decode(body, type, contentType)` and `AceMq.connect(..., Codec)`.
- Serialisation that needs saying nothing. `publisher.send(new OrderPlaced(...))` is
  JSON, and a consumer reads whatever format arrived. Publishers write one format,
  chosen once: `.asJson()`, `.asXml()`, `.asYaml()`, `.asText()`, `.asBytes()`,
  `.as(myCodec)`.
- A codec registry discovered with `ServiceLoader`, the way transports already are.
  `acemq-amqp-codec-json` is a required dependency of the core, so the default never
  depends on the classpath; `acemq-amqp-codec-xml` and `acemq-amqp-codec-yaml` are
  optional, and asking for a format that is not installed names the artifact to add.
- `mq.publisher(exchange, routingKey, PayloadType.class)`, so the payload type is
  real rather than inferred from whatever the result is assigned to.
- `acemq-amqp-codec-avro` and `acemq-amqp-codec-protobuf`. Neither is chosen by name:
  `publisher.as(AvroCodec.registered(registry))`,
  `publisher.as(ProtobufCodec.of(Order.parser()))`. Avro can fix one schema or write
  a Confluent-compatible schema identifier into each message; `SchemaRegistry` is a
  two-method interface so any registry fits behind it.
- `ConsumerOptions.as(codec)`, for the one case where a consumer has to be told the
  format: bytes that describe nothing cannot be recognised on arrival.
- Streams. `mq.declareStream(name, maxAge, maxLengthBytes)` and a reader that says
  where to start: `mq.stream("orders.log", Order.class).fromFirst().consume(handler)`,
  or `.fromOffset(n)`, `.from(instant)`, `.fromLast(Duration)`, `.fromNext()`.
- `StreamConsumer` is a separate type from `MessageConsumer`, because a stream has no
  dead-letter queue, no requeue and no destructive read. A failing handler stops the
  reader, or skips and counts with `.skipFailures()`; there is no third answer.
- `TransportConnection.subscribe(queue, prefetch, consumerArguments, listener)`, which
  is where a stream's starting offset has to live.
- `acemq-security-api`: `Security`, `Credentials` and `CredentialsProvider`, with no
  third-party dependencies. `amqps://` now resolves and verifies the certificate chain
  and the hostname with nothing configured; `amqp://` to anything but loopback logs a
  warning naming the host.
- Relaxations are named methods — `Security.insecure()`, `Security.disabled()` — never
  a boolean in a properties file, so every use is one search.
- `CredentialsProvider` is consulted on every connection rather than once at start-up,
  because tokens expire and automatic recovery reconnects.
- A certificate carrying the development marker is refused unless
  `allowDevelopmentCertificates()` is called.
- Consumer groups, resized while the application runs:
  `mq.consumeGroup(queue, Type.class, handler).concurrency(4).prefetch(50).start()`,
  then `group.scaleTo(8)` and `group.prefetch(100)`. Neither number needs a redeploy.
- `MessageConsumer.drain(timeout)` stops taking new work and waits for what is in
  hand, and `inFlight()` says how much that is. Scaling down drains rather than
  cancelling, so a handler mid-message is not abandoned to a redelivery.
- `Subscription.setPrefetch(int)` on the transport SPI, implemented for RabbitMQ with
  `basic.qos` on the live channel.
- Ordered-per-key delivery. `mq.ordered("orders", Order.class).partitions(8)
  .keyedBy(Order::customerId).declare()` gives ordering within a key and parallelism
  across keys: one queue per partition, exactly one consumer each.
- `Partitioning` — FNV-1a over UTF-8, specified rather than borrowed, because a port in
  another language has to compute the same partition or it reorders messages silently.
- `OrderedQueue.OnFailure` — `STOP`, `RETRY_IN_PLACE` or `SKIP`. The retry ladder is
  not offered here, because republishing a failed message to come back later is what
  breaks a sequence.

### Fixed

- The in-memory transport's `close()` used `shutdownNow()`, interrupting handlers that
  were still running. That contradicted the subscription contract, turned a clean stop
  into a redelivery, and made draining impossible to build on.

### Fixed

- The text codec no longer publishes an object's `toString`. Sending a POJO used to
  put `OrderPlaced@4b1210ee` on the wire: published, confirmed, and useless to
  whoever read it, with nothing anywhere reporting a problem.

### Fixed
- A resource leak reported by ErrorProne as an error: two OpenTelemetry context
  scopes were opened per message. They are in fact closed, by `SpanScope`, which
  the analyser cannot see across; both sites are annotated with the reason.
- Two `Future` values were discarded in the in-memory transport. An exception
  escaping the dispatch loop or a queue expiry would have been captured in the
  unread future and lost, stopping consumption or stranding a message in a retry
  rung with nothing to show why.
- Two Javadoc comments had been left stacked on the same method by an earlier
  edit, so the real documentation was silently discarded.
- Dead fields in the in-memory broker, and an implicit long-to-double conversion
  in the retry schedule.
- Integration-test coverage was never recorded. JaCoCo attaches only to Surefire
  unless `prepare-agent-integration` is bound, and both agents default to writing
  the same `argLine` property, so one silently replaced the other. Failsafe now
  reads its own property, and with `@{}` rather than `${}` so it is evaluated at
  execution time instead of being interpolated to the empty default when the
  model is built.
- The coverage gate was set to zero and enforced nothing at all.
- Failsafe was configured but never bound, so `*IT` tests were skipped while the
  build reported success.
- JaCoCo raised to 0.8.15; 0.8.12 cannot instrument Java 25 class files, and its
  failure surfaced misleadingly as Testcontainers being unable to find Docker.
- Testcontainers raised to 1.21.4; 1.20.4 negotiates Docker API 1.32, which
  Docker Engine 29 rejects.
- The RabbitMQ transport now converts `LongString` header values to `String`, so
  application header comparisons behave as written.
- The publisher stamps `x-acemq-origin` when a caller supplies an envelope
  without one, so no message is published unattributed.
- Optional telemetry dependencies are no longer named in any method that runs
  unconditionally. Naming one, even as a local variable or a method parameter,
  makes the JVM resolve it when the class or method is first used, so the guard
  meant to protect it never runs and the first connection in an application
  without that dependency fails with `NoClassDefFoundError`.
