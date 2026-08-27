# acemq-java-amqp

[![ci](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/attribution-guard.yml)
[![version](https://img.shields.io/badge/version-0.0.1--SNAPSHOT-blue)](https://github.com/AceMQ-Company/acemq-java-amqp/packages)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Java](https://img.shields.io/badge/Java-11%2B-orange)](#requirements)
[![brokers](https://img.shields.io/badge/brokers-RabbitMQ%20%7C%20Qpid-lightgrey)](#requirements)

Broker-portable AMQP library for Java. Reliable publishing, non-blocking retries,
topology planning, and distributed messaging patterns as first-class types —
against RabbitMQ (AMQP 0-9-1) and AMQP 1.0 brokers such as Apache Qpid.

> **Status: pre-release (`0.0.1-SNAPSHOT`).** Nothing is published yet and the
> public API changes without notice. The current goal is a walking skeleton: one
> working path from publish through the broker to consume and acknowledge, proven
> against a real RabbitMQ, before any breadth is added.

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
- **And it does not cost you** — a confirmed publish through AceMQ measures
  within noise of the same publish written by hand against the RabbitMQ client
  (421 against 422 microseconds in the run recorded in `benchmarks/results`).
  A nightly job re-measures it and fails if the gap exceeds five percent.

## Modules

| Module | Contents |
|---|---|
| `acemq-amqp-api` | The public API. No third-party dependencies, by design — every language port is transliterated from it |
| `acemq-transport-spi` | What a broker binding must implement |
| `acemq-amqp-core` | The protocol-agnostic engine: publisher, consumer runtime, retry ladder, topology planner, codec registry, interceptors, telemetry |
| `acemq-amqp-patterns` | Outbox, idempotent consumer, saga, claim-check, request-reply, scheduling |
| `acemq-transport-rabbitmq` | AMQP 0-9-1 binding over the RabbitMQ Java client |
| `acemq-amqp-test` | In-memory transport (`memory://`), Testcontainers harness, fluent assertions |
| `acemq-amqp-codec-json` | JSON, via Jackson. A **required** dependency of the core: the format an application writes should not depend on what happens to be on its classpath |
| `acemq-amqp-codec-xml` | XML, for the parts of an estate that will not be rewritten. External entities disabled and not configurable |
| `acemq-amqp-codec-yaml` | YAML, for messages a person reads as well as a program |
| `acemq-amqp-codec-avro` | Avro, with a fixed schema or a Confluent-compatible schema identifier per message |
| `acemq-amqp-codec-protobuf` | Protocol Buffers, one codec per message type |

`acemq-transport-amqp10` (Qpid Proton-J) joins at milestone M3.

## Serialisation

Publishing an object needs nothing said about it:

```java
try (AceMq mq = AceMq.connect("amqp://localhost")) {
    Publisher<OrderPlaced> orders = mq.publisher("orders", "order.placed", OrderPlaced.class);
    orders.send(new OrderPlaced("o-1", 42.00));                    // JSON

    mq.consume("orders.new", OrderPlaced.class,
            message -> service.accept(message.payload()));         // reads whatever arrived
}
```

A publisher writes one format, chosen once where the destination is named. A consumer
says nothing about format at all — the content type on each message picks the codec,
which is what lets a producer change format without a consumer change.

```java
mq.publisher("legacy", "order",  Order.class).asXml();
mq.publisher("fleet",  "config", Config.class).asYaml();
mq.publisher("files",  "upload", byte[].class).asBytes();
mq.publisher("orders", "placed", Order.class).as(new JsonCodec(myObjectMapper));
```

Avro and Protobuf have no `asAvro()` and no `asProtobuf()`, because neither can be
built without a schema — a method taking no arguments would have nothing to work with:

```java
publisher.as(AvroCodec.registered(registry));
publisher.as(ProtobufCodec.of(OrderPlaced.parser()));

// and, uniquely, the consumer has to be told as well: bytes that describe
// nothing cannot be recognised on arrival
mq.consume("orders.new", OrderPlaced.class,
        ConsumerOptions.defaults().as(ProtobufCodec.of(OrderPlaced.parser())), handler);
```

Adding a format is a `Codec`, a `CodecProvider` and a service file. Asking for one
that is not installed names the artifact to add.

## Requirements

|  | Version |
|---|---|
| Bytecode target | **Java 11** — so Spring Boot 2.7 applications on Java 11 can consume this |
| Build toolchain | **JDK 17 or newer** (CI builds on 17, 21 and 25) |
| Maven | 3.8+ |
| Brokers tested | RabbitMQ 3.13 and 4.x, standalone and clustered at 3, 5 and 9 nodes; Apache Qpid Broker-J from M3 |

Java 21 features such as virtual threads are delivered through multi-release JAR
entries and selected at runtime, so modern deployments benefit without the
baseline rising.

## Building

```bash
mvn clean verify        # build, unit tests, integration tests, coverage, format
mvn spotless:apply      # fix formatting
mvn clean verify -DskipITs   # skip the tests that need Docker
```

Most tests need no broker at all: `acemq-amqp-test` provides an in-memory
transport behind a `memory://` URL that implements the same SPI, so the engine
can be exercised in milliseconds. Integration tests that do start real brokers
with Testcontainers require a running Docker daemon. On macOS with Docker Desktop the socket is not where
Testcontainers looks by default, and the resulting error misleadingly claims no
Docker environment exists:

```bash
export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"
```

Clusters of three nodes are started by the build itself. Five and nine node
clusters run nightly, and can be driven by hand:

```bash
docker compose -f compose/cluster-5.yml up -d --wait
./compose/join-cluster.sh 5
mvn verify -pl acemq-transport-rabbitmq -Dit.test=LargeClusterIT \
    -DfailIfNoSpecifiedTests=false -Dacemq.cluster.size=5
docker compose -f compose/cluster-5.yml down -v
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
  <version>0.0.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-transport-rabbitmq</artifactId>
  <version>0.0.1-SNAPSHOT</version>
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
