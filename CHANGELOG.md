# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Security
- **Dependency updates for 21 Dependabot alerts**, surfaced the moment scanning
  was switched on. All are dependencies consumers inherit:

  | | | |
  |---|---|---|
  | `com.rabbitmq:amqp-client` | 5.25.0 | 5.33.1 |
  | `com.fasterxml.jackson.core:*` | 2.18.2 | 2.18.9 |
  | `org.postgresql:postgresql` | 42.7.5 | 42.7.12 |
  | `org.assertj:assertj-core` | 3.27.0 | 3.27.7 |
  | `org.bouncycastle:bcpkix-jdk18on` | 1.79 | 1.84 |

  The full suite passes, integration tests included, which is the part that
  mattered: the broker client moved eight minor versions.

  **Micrometer is deliberately not bumped.** The advisory is a denial of service
  in its HTTP server instrumentation, which this library does not use — it
  records meters and nothing else — and there is no patched release in the
  `1.14.x` line. It is also an `<optional>` dependency, so the version declared
  here is not inherited: an application chooses its own.

### Fixed
- **Every successful release was announced in Slack with roadworks instead of a
  tick.** The icon expression read `result == 'success' && '' || ':construction:'`,
  intending to fall back on the status-derived tick by passing nothing. In
  GitHub's expression language an empty string is falsy, so `a && '' || b` always
  yields `b`. Both branches now name an emoji. The title was unaffected, because
  its success value is a non-empty string — which is why this survived several
  releases.
- **The release did not rebuild the documentation site.** A push made with
  `GITHUB_TOKEN` does not trigger workflows, so the last docs build was always
  the one for the release commit — before the version rewrite — and the published
  guide advertised the previous version. `0.2.9` shipped saying `0.2.8` until the
  site was rebuilt by hand. The `document` job now dispatches `docs.yml`.

### Changed
- The getting-started page and tutorial 1 showed only
  `declareExchange`/`declareQueue`/`bind`, which is what every AMQP client
  offers, and never mentioned the `Topology` builder. Both now follow the three
  calls with the value form and what it buys: a printable plan, `VALIDATE`, and
  drift caught before anything is declared. The imperative calls stay where they
  are teaching what the three things are.

## [0.2.9] - 2026-09-02

### Added
- **Payload encryption**, in a new `acemq-amqp-crypto`. `EncryptedCodec.wrapping(
  codec, keyring)` puts AES-GCM around any codec, so the broker holds ciphertext
  and choosing a format stays independent of choosing to encrypt. `security.md`
  documented the absence of this; it now documents the thing.

  The key identifier travels in the message rather than in an AMQP header. A
  header would have been tidier and would have lost it — headers are dropped by
  shovels, rewritten by federation, and absent from a message recovered out of a
  backup, and a ciphertext whose key nobody can name is gone. It is authenticated
  as associated data, so rewriting it fails to decrypt rather than quietly
  decrypting as something else.

  `EncryptedCodec.keyIdOf(body)` reads which key a message needs without holding
  any key, because the first consequence of turning this on is a dead-letter
  queue nobody can triage.
- **Topology drift detection.** A queue that exists with settings the topology
  disagrees with is now reported as `DRIFT` rather than as present, and `apply`
  refuses before declaring anything instead of failing partway through with a
  channel-level protocol error.

  AMQP cannot read a queue's arguments back, so the check offers the declaration
  to the broker on a channel of its own and reads the refusal — RabbitMQ's 406
  names the argument and both values, which is more than an inspection API would
  have given. The queue is asked about passively first, so a plan never creates
  the topology it was only supposed to report on.

  Resolving drift is still yours: the safe migration order depends on whether the
  queue can be drained and whether its messages can be lost, which is not a
  library's decision. New `docs/topology.md` covers both ways out.

  Transports add this by overriding `TransportConnection.checkQueue`. The default
  reports `UNSUPPORTED`, which the plan shows as `UNKNOWN` rather than as
  agreement. The in-memory broker implements it, so drift is caught by a unit
  test rather than only against a container.
- **`JdbcSchemaRegistry`**, in `acemq-amqp-patterns`. Until now the only registry
  was `InMemorySchemaRegistry`, which forgets every identifier on restart — and
  since the identifier travels in the message rather than the schema, forgetting
  makes every Avro or Protobuf message written before the restart unreadable,
  silently. This one keeps them in a table, so they survive a restart and are
  shared between replicas.

  Registration is idempotent by content: the same schema offered from eight
  replicas at once yields one row and one identifier. Identifiers come from a
  locked counter row rather than from `MAX(id) + 1`, because two writers
  registering two *different* schemas both compute the same next identifier and
  neither can win by retrying — the loser is racing someone doing the same
  arithmetic. Registration happens once per schema in the lifetime of a system,
  so serialising it costs nothing worth measuring.

  Both directions are cached and never invalidated, since neither answer can
  change; without that it would be a database round trip per message.
- **Request/reply, the outbox relay and pipelines now report what they do.** All
  three kept counters readable only by calling a getter on the object, which no
  dashboard can do — a number that exists and cannot be scraped is a number
  nobody has during an incident.

  | | |
  |---|---|
  | `acemq.request.duration` / `.total` | The round trip as the caller experienced it, `answered` or `timed_out` |
  | `acemq.outbox.lag` / `acemq.outbox.total` | How long a record waited between commit and publish |
  | `acemq.pipeline.run.duration` / `.total` | Message age on leaving, tagged `pipeline`, `step`, and `completed` or `ended_early` |

  **`acemq.outbox.lag` is the one that earns its place.** A committed,
  unpublished row is a message that exists, is owed to somebody, and appears in
  no queue depth anywhere; a stopped relay is indistinguishable from a quiet
  system until this is measured.

  `acemq.request.duration` exists because neither span that already covered a
  request/reply call was what the caller waited for — the publish is timed and
  the reply's delivery is timed, and "how long did asking take" was the gap
  between them. It traces as a `CLIENT` span with both as children. Only the
  blocking `request(...)` is timed: `requestAsync` returns a future with no
  timeout, so the wait belongs to its holder and a scope closed at return would
  time the publish and call it the round trip.

  **The four new `Telemetry` methods are `default` no-ops and will stay that
  way.** That interface is implemented by applications with their own monitoring,
  and an abstract method added after the fact breaks every one of them at compile
  time for a signal they never asked for. A test asserts that a sink implementing
  only the original four still compiles and works.
- **GraalVM native image support**, which turned out to mean documenting rather
  than building. Verified by compiling an image and running it on GraalVM CE 21
  and 25: codec discovery through `ServiceLoader`, the in-memory transport, the
  RabbitMQ transport with confirms, AES-GCM encryption, the schema registry
  including the `.sql` file it reads out of the jar, and TLS with PKCS12 all work
  with **no reachability metadata from the library**.

  So none is shipped. Configuration that testing shows is unnecessary is a
  promise to maintain something nobody reads.

  What an application must do is register its own message types, because Jackson
  reaches their accessors by reflection and nothing names them — the image builds
  and then fails on the first publish. `docs/native-image.md` has the file and
  the tracing-agent command that writes it.

  `mvn -Pnative clean verify` builds `acemq-amqp-native`, a real image running
  real checks, and a nightly job runs it on both JDKs. Two details make it a gate
  rather than decoration: `--no-fallback`, since a fallback image passes by
  bundling a JVM; and `clean`, since without it the plugin reuses the previous
  binary and reports a pass for code that was never compiled. The second one was
  found the hard way while writing the test.
- **`PipelineBuilder.describedAs(String)`** — a sentence saying what a step is
  for, carried into the declaration log line and anywhere a step is reported.

  Separate from the step name rather than folded into it, because the name is
  wire format: it is the routing key, the queue suffix and the routing slip
  entry, so it is restricted to letters, digits, dashes and dots, and renaming a
  step strands every message in flight against the old name. A description has no
  such job, so improving how a stage reads can never move its messages.
- **Tutorials**, at `/tutorials.html`, reachable from a new top-level navigation
  link. Four of them, in order, each ending in something that runs: a first
  message, surviving failure, never processing twice, and seeing what happens.
  The guide explains how a thing works; these start from nothing and finish with
  a working service.
- **A Patterns page and an Observability page.** Both features existed and
  neither was documented, which for the second one meant people reasonably
  assumed there was no instrumentation at all.

### Fixed
- **The README claimed AMQP 1.0 support that does not exist.** The badge read
  "RabbitMQ | Qpid" and the opening sentence said "against RabbitMQ (AMQP 0-9-1)
  and AMQP 1.0 brokers such as Apache Qpid" — present tense, in the most-read
  line in the project — while the module table and the requirements table
  correctly said `acemq-transport-amqp10` arrives at M3. There is one transport.
  Same fault as the patterns claim below and worth the same treatment.
- **The README advertised patterns that do not exist.** It listed saga,
  claim-check and scheduling among `acemq-amqp-patterns`, and an
  `IdempotentConsumer` type, none of which were ever built — the module contains
  the outbox and its relay, two idempotency stores, and the schema registry. The
  same claim was in the module's own POM description. A missing feature somebody
  knows about is a decision; one they discover at integration time is an outage.
- `acemq-amqp-codec-toml` was missing from the coverage aggregator, so the module
  shipped in `0.2.8` with its coverage uncounted.
- `request-reply.html` was missing from the documentation navigation, so the page
  shipped in `0.2.7` was reachable only from the index.
- **`RetryLadderIT` leaked a retry queue between tests.** Teardown deleted
  `orders.new.retry.1s`, but the test that proves a waiting message does not hold
  up the queue behind it builds a five-second ladder — so `orders.new.retry.5s`
  survived, still holding a poison message with a live time-to-live. When it
  expired the broker routed it back into `orders.new`, which by then belonged to
  whichever test ran next: one extra delivery, in a test that counted deliveries,
  attributable to nothing. Teardown now covers every rung, and deletes rungs
  before the queue they feed.

  Two smaller races went with it. The first test incremented a counter and then
  wrote the list it guarded, and waited on the counter — so the wait could be
  satisfied while the list was one element short. It now waits on the list. And
  the `until(x == n)` waits are `>= n`: a counter that overshoots between polls
  never satisfies an equality, turning a wrong answer into a timeout that says
  nothing about what was wrong.

### Security
- **Secret scanning and push protection are on**, along with Dependabot alerts,
  Dependabot security updates and CodeQL, across every public AceMQ repository.
  All were off. Push protection is the one that earns its place: it refuses the
  push rather than reporting the credential after it is public, and a credential
  in a public repository's history is compromised whether or not the commit is
  reverted.

  A grouped weekly Dependabot configuration comes with it. Ungrouped it produces
  a pull request a day, and a queue of ignored bot pull requests trains everyone
  to skim past the one that mattered. Jackson's modules are grouped because they
  must move together — a mixed databind family fails at runtime, not at compile,
  which this project has already paid for once.

  Private repositories are untouched: secret scanning there needs paid GitHub
  Secret Protection, which is a billing decision rather than a build one.

## [0.2.8] - 2026-09-02

### Added
- **TOML**, in `acemq-amqp-codec-toml`, reachable as `Codecs.byName("toml")`. For
  the same audience as YAML — a message a person edits and a machine consumes —
  with the ambiguity removed: one way to write a string, no significant
  indentation, and `country = NO` is an error rather than a boolean.

  It refuses a payload whose top level is not an object. That check exists
  because Jackson does not fail: given a list it writes ` = ['a', 'b']`, a
  key-less assignment that is not TOML and that its own parser rejects with "Got
  KEY_VAL_SEP, expected key or table". Left alone it would publish messages
  nothing can read, discovered by the consumer rather than the publisher.
- A test for `AvroCodec.of(Class<? extends SpecificRecord>)`, which had none. It
  is the same decode path whose generic half silently returned wrong values
  before `0.2.5`, so the specific half being uncovered was the least comfortable
  gap in the module. Covered with a hand-written `SpecificRecord` rather than by
  adding code generation to every build for one class.

## [0.2.7] - 2026-09-02

### Added
- **Request and reply.** `mq.requester()` asks and waits; `mq.respond(queue, type,
  handler)` answers. Replies are matched by correlation id, so one requester can
  have many questions in flight, and the reply queue is deleted on close and
  carries `x-expires` so a killed process does not leave one behind.

  The reply address travels as AMQP's own `reply-to` property rather than an
  `x-acemq-*` header, so a service written against this library can answer a
  caller that was not.

  `RequestTimedOutException` says what a timeout does **not** mean: the request
  may still be queued, being handled, or already done with the reply lost coming
  back. Retrying is a decision about idempotency rather than a reflex.

  Four counters worth graphing: `timedOut`, `unmatched`, `answered`,
  `unanswerable`. `unmatched` rising alongside `timedOut` is the signature of a
  timeout that is too short rather than anything broken.

  The documentation leads with when *not* to use it: request/reply over a broker
  is synchronous calling in asynchronous clothes, and where two services can
  speak HTTP or gRPC they should.
- `Message.replyTo()` and `Message.contentType()`, both defaulting to empty, so a
  consumer can tell a request from a plain message.

## [0.2.6] - 2026-09-02

A capability is a promise that *this library* can do the thing — not that the
broker could if somebody wrote the code. Three were claimed with no API behind
them, so `supports(...)` returned true and left the caller with nothing to call.

### Added
- `PublishOptions.withPriority(int)`, and priority on the wire. `PRIORITY` was
  claimed by the RabbitMQ transport and there was no way to set one. Proven
  against a real broker: an urgent message published last is delivered first,
  ahead of four queued before it.

### Changed
- The RabbitMQ transport **no longer claims `TRANSACTIONS`**. RabbitMQ has
  `tx.select`; this library offers no way to reach it, and publisher confirms
  cover what almost every caller wants transactions for at a fraction of the
  cost, with the transactional outbox covering the rest. Claiming it was the
  dishonest option.
- The in-memory transport **refuses a publish carrying a priority** rather than
  ignoring it. Silently dropping it means a test that passes and a production
  that reorders.

`SINGLE_ACTIVE_CONSUMER` stays claimed: it is reachable today as the
`x-single-active-consumer` queue argument, which is a declaration rather than a
method.

## [0.2.5] - 2026-09-01

### Fixed
- **The outbox relay published a payload nothing could read as a typed event.**
  An outbox stores an already-serialised payload — that is what makes it safe to
  write inside the caller's transaction — and the relay republished it *through
  the ordinary codec*, encoding it a second time. What arrived was a JSON string
  containing JSON, so `consume(queue, OrderPlaced.class)` failed with "no
  String-argument constructor" and the only thing able to read an outbox queue
  was a consumer taking `String` and parsing by hand. The relay now writes the
  stored bytes unchanged, with `application/json`.

  **This changes the wire format of outbox messages.** A consumer that worked
  around the old behaviour by taking `String` needs to either ask for the event
  type — which is the point — or keep taking `String` with
  `ConsumerOptions.as(Codecs.byName("text"))`.
- A NUL byte in `OutboxRelay`'s source, used as a map-key separator
  (`exchange + '\0' + routingKey`). It compiled, and it made the file register as
  binary to git, grep and every diff tool that sniffs content. Now a space.

## [0.2.4] - 2026-09-01

### Added
- `AvroCodec.registered(registry, readerSchema)`, which reads every message
  against a schema of your own rather than the writer's. This is what schema
  evolution needs on the generic path: a field the reader does not know is
  skipped, and one the writer omitted is filled in from the reader's default, so
  a consumer sees the shape it was written against whichever version produced the
  message. Previously only a generated `SpecificRecord` could supply a reader
  schema, and a `GenericRecord` asks for nothing in particular.

### Fixed
- **A fixed-schema `AvroCodec` silently misread messages written by a registered
  one.** The two framings differ by five bytes at the front, Avro does not
  notice, and the decode returned a record whose every field was wrong -- an
  empty id and a total of `5.4e-67` -- without throwing. `canDecode` also
  accepted the other framing's content type, so a consumer would pick the wrong
  codec on its own. Each codec now accepts only its own framing, and decoding
  identifier-framed bytes with a fixed schema fails with an explanation.
- The javadoc examples on `AceMq.pipeline` and `PipelineBuilder` called
  `RetryPolicy.exponential(int, Duration)`, which does not exist — the shortest
  overload takes a maximum delay as well. The snippet a reader copied did not
  compile.

## [0.2.3] - 2026-09-01

### Changed
- Releases are cut from the tag by the release workflow rather than published by
  hand, and the workflow now verifies the published version resolves from an
  empty local repository before reporting success. `0.2.1` went out without the
  fix it was named for because it was built from a working tree; this is the
  check that would have caught it.
- Releases, failed releases and broken example builds announce themselves in
  Slack.

### Removed
- `0.2.1` is deleted from the Maven repository, metadata included. It was
  published without the keystore password change it was named for. Use `0.2.2`
  or later.

## [0.2.2] - 2026-09-01

### Fixed
- Re-releases the keystore password change. `0.2.1` was published from a build
  that did not contain it: the artifacts went out without
  `Security.DEFAULT_KEYSTORE_PASSWORD`, so `fromKeystore` still defaulted to the
  five-character value while the documentation described the new one. Use
  `0.2.2`; `0.2.1` is best avoided.

## [0.2.1] - 2026-09-01

### Fixed
- The default keystore password is now `acemq-dev` rather than `acemq`. Five
  characters meant `keytool` refused to create a PKCS12 keystore with it, so the
  library's own default described a store the standard JDK tooling could not
  produce — a default nobody can use is worse than no default. It is exposed as
  `Security.DEFAULT_KEYSTORE_PASSWORD`, and `acemq-security-dev` writes stores
  with the same value, so `Security.fromKeystore(dir)` now needs no password at
  all for generated certificates.

  A keystore created with the old default still opens: pass
  `keystorePassword("acemq")`.

## [0.2.0] - 2026-09-01

### Added
- `acemq-security-dev`, a Maven plugin that writes throwaway TLS certificates for
  local development in one command:
  `mvn org.acemq:acemq-security-dev:certs -Dbroker=localhost -Dout=./certs`.
  It produces a certificate authority, a broker certificate carrying subject
  alternative names, a client key pair, the two keystores
  `Security.fromKeystore(...)` reads, and the matching `rabbitmq.conf`. The
  library's error messages already pointed at this artifact; now it exists.
  Everything it writes is marked, expires in thirty days, and the goal refuses to
  run when `ACEMQ_ENV` names production.
- A security page in the documentation site, which had none at all despite
  security being a stated principle of the project.

## [0.1.0] - 2026-09-01

The first published release. Everything below has been proven against a real
RabbitMQ in continuous integration, on both 4.x and 3.13.

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
- Taking a connection out of rotation: `mq.drainConsumers(timeout)`,
  `mq.pauseConsuming()` / `resumeConsuming()`, `mq.pausePublishing()` /
  `resumePublishing()`, `mq.inFlight()`. The two directions are controlled
  separately, because a service being drained still has requests to finish and those
  requests still publish.
- `PublishingPausedException`, its own type so a caller can tell "not now" from a
  broker rejection.
- `Subscription.cancel()` on the transport SPI — stop delivery without waiting, which
  is what makes a bounded drain possible.
- Pipelines: `mq.pipeline("fulfilment", Order.class).step(...).step(...).build()`. Each
  step is its own queue with its own retry, concurrency and idempotency, so a slow step
  scales without touching its neighbours.
- `Step<I, O>` returns the next payload, which lets the builder thread types along the
  chain; returning null from a non-final step ends the route.
- `RoutingSlip` — where a message is going, carried by the message. No coordinator, and
  a dead-lettered message keeps its slip so a replay resumes rather than restarts.
- `Envelope.route()`, because the AceMQ header prefix is closed to application headers
  and a slip put there by hand was silently dropped.

- Pipelines. `mq.pipeline(...)` chains steps through the broker, each hop a real
  queue, so a stage that fails is retried and dead-lettered on its own rather
  than taking the whole chain with it. The route travels in a routing slip on
  the message, which makes the chain choreography rather than orchestration.
- Replay. `mq.replay(queue)` moves messages out of `<queue>.dlq` — or
  `<queue>.parked` via `parked()` — back to the queue they failed in. Bounded
  batches, an optional filter, and `pending()` to look before touching. A
  dead-letter queue nobody can drain is a slower way of losing data.
- `Envelope.replayedFrom()`, `replayedAt()` and `replayCount()`. First-class
  fields rather than headers, because engine-owned headers are stripped before
  an application sees them: an audit trail written as a header would have been
  invisible to the code handling the message.
- Blocked connections. RabbitMQ stops reading from publishing sockets under a
  memory or disk alarm without closing them or returning an error, so a
  publisher awaiting confirms waited forever with nothing in the logs. Publishes
  now wait a bounded `blockedTimeout` and then raise `ConnectionBlockedException`,
  which carries the broker's own reason and whether the message may already have
  arrived. `AceMq.isBlocked()` reports it for a readiness probe.
- `PublishOptions`: transient delivery, unroutable tolerance, and per-message
  expiry, per publisher or per message. The defaults stay the safe ones.
- Asynchronous and batch publishing. `sendAsync` returns a future and `sendAll`
  publishes a whole batch before awaiting any of it, which is materially faster
  than a round trip per message. Outstanding publishes are bounded, because an
  async publisher with no ceiling is a memory leak that looks like throughput.
- `JdbcIdempotencyStore`, shared by every consumer pointing at it. A claim is a
  lease rather than a lock: without an expiry, a consumer that dies mid-handler
  would leave a claim that discards every future redelivery of that message, and
  one crash would become silent message loss.
- An interceptor chain. `mq.intercept(...)` runs application code around every
  publish and every handler, for the things every message in an organisation
  needs and no library can guess. Refusing a publish, or a delivery, is part of
  the contract.
- `TransportConnection.receive` and `messageCount`, the pull and the depth that
  draining a queue needs. A subscription is never told that no more messages are
  coming, so a drain built on one either stops early or hangs on an empty queue.
- A user guide and aggregated Javadoc published to GitHub Pages, and artifacts
  published to a Git-hosted Maven repository that needs no credentials to read.

### Fixed

- `@apiNote`, `@implSpec` and `@implNote` are declared to the Javadoc plugin.
  They are standard tags, but Javadoc only recognises them for the JDK's own
  build unless a project declares them; undeclared, they were "unknown tag"
  errors that failed the Javadoc jar and so failed any release, while every
  ordinary build passed.
- The outbound message is built inside the telemetry scope. Trace context is
  read from the current span, so gathering those headers before the publish span
  existed propagated the caller's span instead, and consumers attached their
  work to the wrong parent — a broken trace that still looked like a trace.
- The in-memory broker honours per-message expiry and stops reporting an
  unroutable message as a failure when the publisher asked not to be told. A
  fake that is stricter than the broker it stands in for fails tests that would
  pass in production, which is the one thing a fake must not do.

- The in-memory transport delivered exactly one more message after a subscription was
  cancelled, because a blocked poll returned a message it had already taken. It is put
  back now.

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
