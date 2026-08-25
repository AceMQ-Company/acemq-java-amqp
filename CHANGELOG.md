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

### Fixed
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
