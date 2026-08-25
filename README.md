# acemq-java-amqp

Broker-portable AMQP library for Java. Reliable publishing, non-blocking retries,
topology planning, and distributed messaging patterns as first-class types —
against RabbitMQ (AMQP 0-9-1) and AMQP 1.0 brokers such as Apache Qpid.

> **Status: early development (`0.x`).** The public API is not yet stable and may
> change in any release. The module skeleton and build gates are in place; the
> engine is being implemented.

## Why

Using AMQP correctly in production is hard for reasons that have little to do with
the wire protocol. Publisher confirms are opt-in, retries are usually implemented
as `Thread.sleep` inside a consumer (which blocks a channel and wrecks prefetch
accounting), dead-lettering is hand-configured per queue, and idempotency and the
outbox are re-invented in every service. Existing clients are good transports;
what is missing is a layer above them where the correct pattern is the shortest
path.

AceMQ aims to be that layer:

- **Correct by default** — confirms on, manual ack, bounded prefetch, dead-letter
  wired, unless you opt out by name.
- **Patterns as types** — `RetryPolicy`, `Outbox`, `IdempotentConsumer`, `Saga` are
  things you configure, not blog posts you re-implement.
- **Portable core, honest edges** — broker-specific features are reachable, never
  silently emulated. A missing capability fails at startup and says so.
- **Retries live in the broker** — a generated tier ladder, so throughput and
  ordering stay analyzable and no consumer thread ever sleeps.
- **Observable from the start** — OpenTelemetry spans and Micrometer metrics are
  emitted by the core, with names fixed by the specification so every language
  port reports identically.

## Modules

| Module | Contents |
|---|---|
| `acemq-amqp-api` | The public API. No third-party dependencies, by design — every language port is transliterated from it |
| `acemq-transport-spi` | What a broker binding must implement |
| `acemq-amqp-core` | The protocol-agnostic engine: publisher, consumer runtime, retry ladder, topology planner, codecs, interceptors, telemetry |
| `acemq-amqp-patterns` | Outbox, idempotent consumer, saga, claim-check, request-reply, scheduling |
| `acemq-transport-rabbitmq` | AMQP 0-9-1 binding over the RabbitMQ Java client |
| `acemq-amqp-test` | In-memory transport, Testcontainers harness, fluent assertions |

`acemq-transport-amqp10` (Qpid Proton-J) joins at milestone M3.

## Requirements

|  | Version |
|---|---|
| Bytecode target | **Java 11** — so Spring Boot 2.7 applications on Java 11 can consume this |
| Build toolchain | **JDK 17 or newer** (CI builds on 17, 21 and 25) |
| Maven | 3.8+ |
| Brokers tested | RabbitMQ 3.13 and 4.x; Apache Qpid Broker-J from M3 |

Java 21 features such as virtual threads are delivered through multi-release JAR
entries and selected at runtime, so modern deployments benefit without the
baseline rising.

## Building

```bash
mvn clean verify        # build, test, coverage gate, format check
mvn spotless:apply      # fix formatting
```

Formatting is enforced with the Eclipse formatter (`etc/eclipse-formatter.xml`)
rather than google-java-format or palantir-java-format: those reach into javac
internals and break on newer JDKs, and a contributor's JDK must produce the same
result as CI.

## Using it

Not yet published. Snapshots will be available from GitHub Packages and tagged
releases from Maven Central, under the `org.acemq` group.

```xml
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-transport-rabbitmq</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

GitHub Packages requires authentication even for public artifacts, so Maven
Central remains the distribution channel for releases.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Work follows a gate model —
**design → code → test → benchmark → release** — in which telemetry and extension
points are exit criteria of the design and code gates, not later additions.

## License

[Apache License 2.0](LICENSE).
