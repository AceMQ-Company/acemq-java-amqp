# acemq-java-amqp

[![ci](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-java-amqp/actions/workflows/attribution-guard.yml)
[![version](https://img.shields.io/badge/version-0.2.8-blue)](https://github.com/AceMQ-Company/acemq-java-amqp/packages)
[![docs](https://img.shields.io/badge/docs-acemq--company.github.io-blue)](https://acemq-company.github.io/acemq-java-amqp/)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Java](https://img.shields.io/badge/Java-11%2B-orange)](#requirements)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#requirements)

Broker-portable AMQP library for Java. Reliable publishing, non-blocking retries,
topology planning, and distributed messaging patterns as first-class types.

**RabbitMQ (AMQP 0-9-1) today.** The core is protocol-agnostic and everything
broker-specific sits behind a transport SPI, which is what makes the second
binding a module rather than a rewrite — but that module does not exist yet.
`acemq-transport-amqp10` (Qpid Proton-J) arrives at milestone M3.

> **Status: `0.2.8`, published.** The public API may still change while the
> version is `0.x`. Everything documented here is proven against a real RabbitMQ
> in continuous integration, on both 4.x and 3.13.
>
> Artifacts are published to <https://acemq-company.github.io/maven/> rather than
> Maven Central, which is a deliberate pre-1.0 choice — see
> [RELEASING.md](RELEASING.md) for the reasoning and the cost.

## Links

| | |
|---|---|
| **Documentation** | <https://acemq-company.github.io/acemq-java-amqp/> |
| **API reference** | <https://acemq-company.github.io/acemq-java-amqp/apidocs/> |
| **Artifacts** | <https://acemq-company.github.io/maven/> |
| Releases | [CHANGELOG.md](CHANGELOG.md) · [tags](https://github.com/AceMQ-Company/acemq-java-amqp/tags) |

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
- **Patterns as types** — `RetryPolicy`, `Outbox`, `IdempotencyStore` and
  `Pipeline` are things you configure, not blog posts you re-implement.
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
| `acemq-amqp-core` | The protocol-agnostic engine: publisher, consumer runtime, retry ladder, topology planner, codec registry, interceptors, telemetry, request/reply |
| `acemq-amqp-patterns` | Transactional outbox and relay, idempotency stores (in-memory and JDBC), JDBC schema registry |
| `acemq-amqp-crypto` | Payload encryption: AES-GCM around any codec, with the key identifier in the message so keys rotate without a flag day |
| `acemq-transport-rabbitmq` | AMQP 0-9-1 binding over the RabbitMQ Java client |
| `acemq-amqp-test` | In-memory transport (`memory://`), Testcontainers harness, fluent assertions |
| `acemq-amqp-codec-json` | JSON, via Jackson. A **required** dependency of the core: the format an application writes should not depend on what happens to be on its classpath |
| `acemq-amqp-codec-xml` | XML, for the parts of an estate that will not be rewritten. External entities disabled and not configurable |
| `acemq-amqp-codec-yaml` | YAML, for messages a person reads as well as a program |
| `acemq-amqp-codec-toml` | TOML, for the same audience as YAML with the ambiguity removed |
| `acemq-amqp-codec-avro` | Avro, with a fixed schema or a Confluent-compatible schema identifier per message |
| `acemq-amqp-codec-protobuf` | Protocol Buffers, one codec per message type |

`acemq-transport-amqp10` (Qpid Proton-J) joins at milestone M3.

## Serialization

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

Published to the AceMQ Maven repository under the `org.acemq` group. It needs no
credentials — add it alongside your other repositories:

```xml
<repositories>
  <repository>
    <id>acemq</id>
    <url>https://acemq-company.github.io/maven/</url>
  </repository>
</repositories>

<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-core</artifactId>
  <version>0.2.8</version>
</dependency>
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-transport-rabbitmq</artifactId>
  <version>0.2.8</version>
</dependency>
```

The [getting started guide](https://acemq-company.github.io/acemq-java-amqp/getting-started.html)
takes it from here: a broker in Docker, a first message, and what the library did
on your behalf that the code does not show.

`acemq-transport-rabbitmq` can be `<scope>runtime</scope>`: it is discovered by
the `amqp://` scheme in your URL, and your code should not compile against it.

Snapshots go to GitHub Packages, which requires authentication even for public
artifacts — which is exactly why releases do not. Maven Central comes at 1.0;
[RELEASING.md](RELEASING.md) explains the trade.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Work follows a gate model —
**design → code → test → benchmark → release** — in which telemetry and extension
points are exit criteria of the design and code gates, not later additions.

## Licence and warranty

[Apache License 2.0](LICENSE). You may use these libraries in production,
commercially, without asking and without paying.

**They are provided "as is", without warranty of any kind**, and the authors and
contributors accept no liability for any damages arising from their use. That is
not a disclaimer bolted on here — it is sections 7 and 8 of the licence itself,
which is the same footing every Apache-licensed dependency you already run sits
on. If you need contractual guarantees, indemnity or a support commitment, those
come from an agreement rather than from a licence: see
[Enterprise support](https://acemq.com).

Nothing here grants trademark rights (licence section 6). **RabbitMQ is a
trademark of Broadcom Inc. and/or its subsidiaries**; AceMQ is an independent
project, is not affiliated with or endorsed by Broadcom, and references to
RabbitMQ describe compatibility only.

## Enterprise support

Commercial support for RabbitMQ and for these libraries — architecture review,
production readiness, TLS and permission design, incident response — is available
from [acemq.com](https://acemq.com). The libraries are complete and free to use
without it.
