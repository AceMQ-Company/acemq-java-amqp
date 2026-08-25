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
