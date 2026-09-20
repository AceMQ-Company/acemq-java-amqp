# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

## [0.7.1] - 2026-09-20

### Security
- **`com.rabbitmq:amqp-client` moves 5.33.1 → 5.36.0, closing CVE-2026-75516
  (GHSA-jh4v-gfqj-7rhx), an unbounded allocation in the client's connection
  handshake.** After `connection.tune`, the client capped the inbound frame
  payload with `Math.min(maxInboundMessageBodySize, frameMax)`. In AMQP a
  `frame_max` of zero means *no limit*, so when the negotiation settled on zero
  that `Math.min` selected the zero and switched off the 64 MB body cap it was
  meant to be tightening. A single frame could then ask for up to
  `Integer.MAX_VALUE` bytes and take the process down with an
  `OutOfMemoryError`.

  **This library was exposed, in the sense that it never opted out.** It sets
  neither `requestedFrameMax` nor `maxInboundMessageBodySize`, so every
  connection it opens inherits the client defaults — and the client's default
  `requestedFrameMax` is zero. Firing the bug needs the *peer* to send zero as
  well, because the negotiation takes the larger value when either side says
  "unlimited". A stock broker ships `frame_max = 131072` and so negotiates
  131072, which is why this is not a thing an ordinary deployment hits; reaching
  it takes a broker configured with `frame_max = 0`, or something impersonating
  one on the wire. A narrower door than the severity suggests — but a door this
  library left unlocked, not one it never built.

  No AceMQ code changes. Applications that take the client version from this
  library get the fix by upgrading; applications that pin
  `com.rabbitmq:amqp-client` themselves have to move to 5.34.0 or later on their
  own, because their pin wins over ours.

  The version is declared in one place, `rabbitmq.amqp.client.version` in the
  root POM, and reaches `acemq-transport-rabbitmq` and `acemq-amqp-benchmarks`
  through `dependencyManagement`. 5.36.0 rather than the minimum 5.34.0 because
  it is the head of the line: the two releases after the fix are maintenance
  only, declare themselves compatible, and harden the same negotiation and
  table-parsing paths further. The dependency shape is unchanged — Netty was
  already a compile-scope transitive of 5.33.1, and comes along at 4.2.18 rather
  than 4.2.15.

  The full suite passes on JDK 21 against RabbitMQ 4.x and again against 3.13,
  which is the part that mattered: a transport bump can move reconnection,
  confirm and dispatch behaviour without a unit test noticing. `HandlerConcurrencyIT`,
  `GracefulShutdownIT` and `BlockedHealthIT` — the consumer dispatch pool, the
  shutdown budget, and blocked-connection handling — are green on both brokers.

## [0.7.0] - 2026-09-20

### Fixed
- **Concurrency is now the number of handlers that actually run at once, rather
  than the number of cores the machine has.** The RabbitMQ transport left the
  client to supply its own consumer dispatch pool, which is a fixed
  `availableProcessors()` threads shared by every channel on the connection. So
  the ceiling on concurrent handlers was the size of the machine, across every
  consumer and every group together, whatever concurrency was asked for: on a
  four-core pod, ten consumers at `concurrency(10)` ran four and the other six
  waited. Nothing said so — the consumers existed, the broker had delivered to
  them, and the handlers had not started.
  That cap is wrong for what concurrency is for here. It is the knob for handlers
  that spend their time waiting on a database or an HTTP call, where the right
  number is the one the caller chose and has nothing to do with cores. The
  transport now supplies a pool that grows on demand and releases threads idle
  for a minute, and closes it with the connection. Unbounded in form only: the
  client dispatches at most one delivery per channel at a time and each consumer
  holds its own channel, so the thread count cannot exceed the number of
  consumers the application itself created.
  Prefetch is unaffected and was never the cause — each consumer already gets its
  own channel and its own `basicQos`, so `prefetch(1)` on a group of two means
  one message per consumer, not one per group.

- **A shutdown budget is now spent once in total rather than once per consumer
  and once per group.** `ConsumerGroup.close()` gave every member the full drain
  timeout in turn, and `AceMq.close()` gave every group its own on top of that,
  so the cost of stopping was the timeout multiplied by however many consumers
  and groups an application happened to have. Nothing chose those totals and
  nothing reported them. Measured against a real broker with three groups of two
  consumers all mid-message and a five-second drain timeout each: **15.047 s
  before, 0.818 s after** against a supplied budget of 800 ms.

  The number this has to fit inside is Kubernetes' default
  `terminationGracePeriodSeconds` of 30, and a budget that multiplies does not
  fit it: eight consumers at the old thirty seconds each came to four minutes,
  at the end of which the pod is killed anyway and every message still held is
  redelivered — the exact outcome draining exists to avoid, arrived at slowly.
  One deadline is now shared by every consumer in a group and by every group on
  a connection, and the default is **20 s, down from 30 s**, to leave room inside
  that grace period for the rest of an orderly shutdown.

  `ConsumerGroup.drain(Duration)` already documented its argument as "how long to
  wait in total" and did not behave that way; it does now. Consumers reached
  after the deadline has passed are still cancelled, just not waited for —
  skipping them would leave consumers taking new work while the application shut
  down around them.

### Added
- **`AceMq.close(Duration)`, which closes the connection inside a budget the
  caller supplies.** `close()` is unchanged in spelling and now means
  `close(Duration.ofSeconds(20))`. Applications that know their own grace period
  — an orchestrator's, a test's — can say so rather than inherit a default
  chosen for somebody else's deployment.
- **Two integration tests that run against a broker under a genuine memory
  alarm, `BlockedHealthIT` and `GracefulShutdownIT`.** Sibling libraries shipped
  a health check that reported a blocked connection correctly in principle and
  hung in practice, because the check asked the broker a question first; a
  blocked connection is one RabbitMQ has stopped reading from, so the question
  never arrived and the careful blocked-aware branch never ran. **Java did not
  have that defect** — the facts a health check reads are answered from state the
  connection already holds — but nothing proved it, and a property nothing tests
  is a property that survives until it does not.

  `BlockedHealthIT` times the five facts a health indicator reads against a real
  `connection.blocked`: **32.6 µs**. It then shows, on that same connection, that
  a passive queue declare had still not returned after 5 s and only completed
  once the alarm cleared — without which the first measurement would pass just as
  happily against a broker that was never blocked, and would prove nothing.

### Changed
- **`docs/publishing.md` says plainly that a health check must not ask the broker
  anything, and `docs/consuming.md` documents the shutdown budget** — that it is
  a total rather than an allowance per consumer, what the twenty seconds is
  measured against, and that a consumer past the deadline is still stopped.

## [0.6.0] - 2026-09-17

### Added
- **A third cross-language fixture, `avro-resolution-fixtures.json`, and a
  `docs/serialization.md` section saying when an Avro message is resolved onto a
  reader schema.** The five libraries do not resolve in the same circumstances
  and the difference reads as a bug until somebody writes down why it is not:
  resolution needs a reader schema, and each library resolves exactly when it
  has one. A Go struct carries no schema, so Go resolves only when the caller
  passes `avro.ReaderSchema(...)`; .NET, Python and Ruby construct their codec
  with a schema, so they have one by default, and each gives the caller a way to
  name another or to decline resolution entirely; Java has one when the target is a
  generated `SpecificRecord` class or the codec was built with
  `AvroCodec.registered(registry, readerSchema)`, and none when a
  `GenericRecord` comes through a plain registry codec. One rule —
  **resolution happens when the library has a reader schema to resolve onto** —
  landing five different ways because the languages can know different things.
  **No resolution behaviour changed in any library.**

  The fixture pins the case that actually bites and its opposite. A field the
  writer removed that the reader declares with a default: resolved, the default
  arrives; unresolved, the field is absent, and a consumer sure it declared that
  field reads nothing. A field the writer added that the reader does not
  declare: harmless either way, and pinned precisely because it is the one
  people assume is dangerous. Each case carries both schemas, the registry-framed
  bytes this library writes, the schema id in the frame, and the decoded value
  under **both** behaviours, named `resolved` and `writerShape`, so a library
  reading the file can assert its own documented behaviour without guessing
  which column applies to it. The default is `"GBP"` and not `""` on purpose: a
  default that is also the type's zero value passes whether resolution happened
  or not.

  Java asserts both columns, being the only one of the five that reaches
  `writerShape` without naming a schema at all: `registered(registry)` is handed
  none, and the writer's schema comes off the wire and serves as the reader's.
  Go, .NET, Python and Ruby take byte-identical copies, as they do for the other
  two fixtures, and each asserts the column it does not land on by default.
- **`docs/streams.md` says that the stream prefetch default is this library's
  choice and not part of the cross-language contract.** It reads as a contract
  when four other libraries document a number next to the same feature, and the
  numbers do not match: Java and .NET default a stream consumer to 100, Go,
  Python and Ruby to 10. Nothing is wrong with either, and **the number here is
  not changing** — prefetch trades memory against throughput, the right answer
  depends on payload size and handler speed, and both are properties of the
  application rather than of the protocol. What the stream contract actually
  covers is the offset, the retention arguments and the message on the wire;
  prefetch is a consumer-side setting that never leaves the channel, so a stream
  written by one library is readable by any of them whatever each defaults to.
  The page now carries the table of all five defaults, says plainly that a
  difference between two languages here is not a bug to file, and shows how to
  state the value rather than inherit it — the same advice the page already gives
  about the reading position, and for the same reason. The other four
  repositories are getting the same framing so the five pages agree.
- **`Envelope.claim()`: the reserved `x-acemq-claim` header is a real envelope
  field, so a claim set by another library no longer vanishes here.** The name
  was defined in `AceHeaders`, referenced by nothing, and written by nothing —
  which meant the engine's reserved-prefix filter did what it does to every
  unrecognised `x-acemq-` header and dropped it on the way in. Python and Ruby
  have carried `claim` as a first-class envelope field all along, so a message
  published by either of them arrived in a Java handler with the claim silently
  gone and nothing reporting the loss. It is now read and written like the other
  reserved fields, with `Envelope.Builder.claim(String)` to set one and
  `Envelope.claim()` returning `Optional<String>` to read one. Go and .NET are
  getting the same field, so all five will agree.

  The semantics are the ones the other optional fields already use: **absent
  rather than empty.** A `null` or `""` claim writes no header at all, matching
  what Go, .NET, Python and Ruby put on the wire, because a header carrying `""`
  is a header somebody has to write a special case for at the other end. It
  participates in `equals`, `hashCode` and `toBuilder`, so it survives a retry
  and a replay like every other field.

  **The claim-check pattern is unaffected and deliberately stays that way.**
  `ClaimCheckCodec` frames its reference in the body and sets no header,
  because a header can be stripped by a shovel or a federation link and because
  a present-or-absent header cannot say whether a payload travelled inline. All
  five libraries make that choice. This field is the *optional* one an
  application may set to tell an operator reading a dead-letter queue where a
  payload went; the engine never writes or interprets it.

  The shared envelope fixtures gain a `claimed` case, since a header on the wire
  that no fixture pins is exactly how this divergence survived unnoticed in the
  first place. **The regenerated `envelope-fixtures.json` needs carrying to
  acemq-go-amqp, acemq-dotnet-amqp, acemq-python-amqp and acemq-ruby-amqp**; a
  fixture updated in one repository and not the other four looks like agreement
  and is worse than no fixture at all.
- **`docs/compatibility.md`: the RabbitMQ 3.13 compatibility matrix, which the
  Status section had listed as the one thing still genuinely open before 1.0 and
  which had never been built.** The library is developed and released against
  RabbitMQ 4.x; 3.13 is what a large part of the installed base actually runs, so
  "does this work on 3.13" had no answer that came from a run. It does now, and
  the answer is that **everything works on both** — the whole
  `acemq-transport-rabbitmq` integration suite, 53 tests, passes unmodified
  against RabbitMQ 3.13.7 and 4.3.6 alike, with no feature here needing 4.x and
  no minimum above 3.13 for anything on the message path. Every row on the page
  came from running against a real broker of each version rather than from
  reading release notes.

  The page is worth more for the four places the brokers genuinely differ, each
  checked rather than assumed, and each with the reason it does **not** reach
  this library written down — because "it happens to pass" and "it cannot break"
  are different claims. RabbitMQ 4 *denies* transient non-exclusive queues where
  3.13 permitted them, and closes the whole connection doing it (`541
  INTERNAL_ERROR`) rather than just the channel; this library never declares one,
  because `durable` is hard-coded true on every builder path in `Topology` and
  `AceMq.declareQueue` has no durability parameter to pass. RabbitMQ 4 records
  `x-queue-type` on every queue where 3.13 leaves it off a classic one, which is
  what makes an argument-equality check report drift on every classic queue after
  an upgrade; drift detection here re-declares and reads the broker's `406`
  rather than comparing argument maps, so the broker decides equivalence and the
  answer is identical on both. Classic queue mirroring is `removed` in 4 — a
  `ha-mode` policy that 3.13 accepts is a `400` there — and this library writes
  no policies at all, so the migration item is the operator's; `declareQueue`
  already defaults to quorum, which is the thing to migrate *to*. And `global_qos`
  is `denied_by_default` in 4, which matters only to code calling `basic.qos`
  with `global=true`; every prefetch this library sets goes through the
  single-argument `channel.basicQos(prefetch)`, which is per-consumer.

  Two differences are listed that are not this library's behaviour but will break
  things around it during an upgrade: the stream-prefetch refusal is worded
  differently on the two versions (this library matches the numeric reply code,
  never the text, but anything string-matching broker errors will not survive),
  and `protocol-listener` health answers differ in shape — `"protocol"` became
  `"protocols"`, and `"missing"` went from a string to an array — so a readiness
  probe reading those fields breaks while one reading the status code does not.

  Linked from the site navigation and from the overview, and the Status section
  no longer lists the matrix as missing. The `compatibility` job in `ci.yml`
  re-runs the integration suite against `rabbitmq:3.13-management` on every pull
  request, so the page is re-checked rather than left to go stale the way the
  claim it replaces did.
- **`docs/envelope.md`: the envelope has a documentation page of its own, which it
  had in the other four libraries and not in this one.** Java documented the
  envelope as six lines of accessors inside the consuming guide: how to read one,
  and nothing about the header contract, the reserved namespace or the defaults —
  which are the three things somebody debugging a message in a dead-letter queue
  needs and the three things the cross-language fixtures actually pin. The page
  states what is always written and what is omitted when unset, that the two
  timestamps on the wire are deliberately encoded differently, why `x-acemq-` is
  reserved and `acemq-` is not, and what each field defaults to — including the
  two edges nothing had written down: `origin` is `{clientName}@{hostname}` rather
  than always `acemq@{hostname}`, and a `type` falling back to an empty routing
  key becomes the literal `message`. It is linked from the site navigation and the
  overview, and the consuming guide now points at it rather than restating a
  fraction of it.

### Changed
- **The docs build fails on a dangling internal link or a dangling anchor, and
  the `.md` → `.html` rewrite no longer drops anchored links on the floor.** The
  site had no link check at all: `docs.yml` installed pandoc, aggregated the
  Javadoc, rendered the pages and uploaded them, and a page pointing at a 404 was
  published without complaint. Go, Python and Ruby all check; Java did not.

  The check lives in `.github/scripts/build-docs-site.sh`, not in the workflow, so
  it runs on a local build too — which is where a bad link costs a minute to fix
  rather than a round trip through CI. It resolves files *and* fragments: every
  `#fragment`, cross-page and same-page alike, has to name a real `id` on the
  target page. Offenders are printed as `page -> href`. `http://`, `https://` and
  `mailto:` are skipped. Links into the generated `apidocs/` reference are checked
  for file existence — a link into a class that no longer exists is a real break —
  but their fragments are not, because those ids are javadoc's, generated from
  erased signatures and encoded differently between JDKs; and the reference's own
  pages are not scanned as sources, since that would report on a generator's
  output rather than on this repository's prose.

  The bug it was written for is real and is fixed in the same change. The rewrite
  that turns cross-page `.md` links into `.html` for the rendered copy was
  anchored on `.md"`, so `guide.md#section` did not match and reached the site
  still pointing at a `.md` file the site does not contain. That was caught by
  hand while `docs/envelope.md` was being written; the hand fix was to write those
  links as `.html#` directly, which is why the defect left no trace in the pages.
  The rewrite now handles the anchored form, `#` is excluded from the path capture
  so a fragment can never be swallowed, and the checker would catch it if it
  regressed — a link surviving as `foo.md#bar` is a page the site does not have.

  **On its first run against the rendered site the check found nothing**: 22
  pages, 1,237 internal links, every file and every anchor resolving, including
  the ten cross-page anchors in `compatibility`, `envelope` and `publishing`. That
  is a clean result and is reported as one rather than dressed up. It was verified
  against deliberately broken input rather than trusted for being quiet: a missing
  page, a dangling cross-page anchor and a dangling same-page anchor are each
  reported and each fail the build with exit 1, while a valid cross-page anchor
  and a link into `apidocs/` pass.

  One thing the check does not cover, found while confirming it: every internal
  cross-page link under `docs/` is written as `.html`, not `.md`, so all of them
  are dead when the same files are read through GitHub's markdown view — the exact
  case the `.md` convention in this script exists to serve. The rendered site is
  correct, which is why a site-scoped check cannot see it. That is closed in this
  release rather than left standing: all 75 links are now written as `.md`, and
  the script gained a source-side check for them — see *Fixed*, below.

- **The overhead budget is 10% of the raw RabbitMQ client, not 5%, and the README
  no longer claims the two are indistinguishable.** A local 3×10 run measured
  `452.7 ±13.2` against `433.8 ±11.6` µs/op: **+4.4%, interval [+0.2%, +8.5%]**.
  Two things follow, and neither is comfortable. The interval no longer contains
  zero, so AceMQ **is** measurably slower than a hand-written confirmed publish on
  that machine, by somewhere between 0.2% and 8.5% — the earlier "not shown to
  differ" reading was a measurement too coarse to see the difference, not an
  absence of one. And with a true overhead near 4.4%, fitting a whole interval
  inside 5% needs about ±0.6% precision, which is roughly 1,400 samples on quiet
  hardware against the 30 taken today; the 5% gate could in practice only fail
  above about 9%. A budget that cannot fail at its stated number is decoration, so
  the stated number is now the one the gate enforces.

  `etc/check-overhead-budget.py` defaults to 10%, the nightly passes `10`, and the
  reproduction line on the benchmarks page says `10`. The ≤200 µs added p99
  latency half of the budget is unaffected and unchanged. The README's performance
  bullet now quotes the measured figure with its interval and names the budget —
  this is the second correction to that bullet, after it quoted `421 against 422
  microseconds` from ±40% error bars.

  `etc/test-benchmark-checks.py` moved with it rather than after it. It now pins
  the default limit at 10%, a run over 10% that fails, and — the case the change
  is actually about — a run at +7.5% with interval [+6.3%, +8.7%], asserted to
  pass at 10% and to fail at 5%. The run behind the decision is a fixture too,
  asserted as *within budget* at 10% and *inconclusive* at 5%. That file exists
  because the gate was silently wrong for eight nights and nothing caught it; a
  threshold that moves without its tests moving is the same failure again.

  The honest caveat is that the measurement design is the real limit. A ~450 µs
  broker round trip dominates the ~19 µs of library overhead being measured, so
  the benchmark is detecting a small difference inside a large number it does not
  control, and no amount of budget arithmetic fixes that. Excluding the round trip
  would make a tighter budget defensible again. That is a separate piece of work.

### Fixed
- **Javadoc that described the pre-0.5.0 header layout.** `Envelope.replayedFrom()`
  still explained itself as a field that exists because engine-owned headers are
  stripped from a handler's view — true when the replay stamps were written under
  `x-acemq-`, and not true since they moved to the shared namespace, where they
  reach the handler as headers as well as fields. `EnvelopeHeaders` carried the
  same vintage in two comments: one calling the routing slip an exception to the
  reserved-prefix filter, when the filter drops the route headers like any other
  and the slip is rebuilt from the delivery's own headers; and one saying Java
  writes the replay timestamp as a number, which it has not done since 0.5.0. No
  behaviour changes; the API reference is generated from these.
- **Two javadoc comments were attached to the wrong thing, and the compiler had
  been saying so.** `Envelope.Builder.header`'s sat above the `route` field, so the
  published reference attached `@throws IllegalArgumentException` — the one warning
  that stops somebody putting a header in the reserved namespace — to a field
  rather than to the method that throws it, and `header` itself appeared
  undocumented; `Message.withPayload`'s sat above `replyTo`'s in the same way. Both
  now sit on the members they describe, and the two
  `documentation comment is not attached to any declaration` warnings are gone.
- **`Responder.answered()` is incremented before the reply is published, so a
  caller holding its answer can rely on the count already including it.** It used
  to be incremented after the send returned, which left a window in which the
  reply was in the caller's hands and the responder still reported that nothing
  had been answered — a monitoring dashboard showing an idle service that was
  demonstrably working, and a number no test could assert without first sleeping.
  It was found the honest way: an example had to sleep before reading the counter,
  and both its READMEs explained why, which is a library defect living in
  documentation. A publish that fails now hands its increment back, so this counts
  replies that were sent rather than replies that were attempted, and the fix does
  not trade one wrong number for another. .NET resolved this first and deliberately
  did not follow Java; Java and .NET are the only two of the five that expose these
  counters at all, and the two now promise the same ordering.
  `unanswerable()` never had the problem — it is counted
  before the delivery is acknowledged, which is the only thing anyone can see — and
  neither did the start-up window .NET had to close, because Java's counters are
  initialised where they are declared and so are in place before the constructor
  calls `mq.consume(...)`.
- **`docs/request-reply.md` promised the responder counters in all five libraries
  and described the reply-address split as current. Neither was true.** The page
  said of `answered` and `unanswerable` that "all five libraries promise this, and
  they promise it identically"; Java and .NET expose them and Go, Python and Ruby
  expose neither, so a responder in three of the five counts nothing at all. Ruby
  goes as far as defining `answered` and `timed_out` as telemetry outcome names
  that nothing writes — a constant no code reaches, which reads from outside as a
  supported feature, and is the same trap `x-acemq-claim` set in this library
  until the change above. The page now names the two libraries that have the
  counters and says what a dashboard covering all five reads instead: the
  responder queue is an ordinary queue, so `acemq.messages.consumed.total` counts
  it, and the request span carries the round trip. Less direct than the counters,
  and it works everywhere.

  The reply-address paragraph described a split that closed two releases ago. It
  said Go, Python and Ruby "have only ever written the header" while Java and .NET
  "only ever wrote the property" — the state before 0.5.0, since when all five
  write both: Go sets `acemq.ReplyTo` alongside the header, Python passes
  `reply_to=` with it, Ruby writes `reply_to:` and the header together, each
  checked in that library's source rather than assumed. The read order is
  unchanged and still header first, and that order is now explained by the split
  it came from rather than presented as a live incompatibility.
- **The overview's Status section understated what ships, in both directions.**
  It listed batch and asynchronous publishing as still to come when both are on
  `Publisher` — `sendAsync` returns a `CompletableFuture<PublishResult>` and
  `sendAll` publishes the whole batch before awaiting any confirm — and listed a
  Spring Boot starter as unbuilt when it released as `0.1.0` in its own
  repository, on its own version line. The capability list was also short of
  request and reply, sagas, claim check, scheduling, payload encryption, topology
  drift detection and native image, and counted five serialization formats where
  six modules are published. A reader deciding whether to depend on this was
  being told less than it does.
- **Three sentences of `avro-resolution-fixtures.json` described libraries that
  have since moved, and the schema-resolution table in `docs/serialization.md`
  overstated three of its five rows.**
  The fixture is generated here and copied byte for byte into the other four
  repositories, so a wrong sentence in it is a wrong sentence in five places, and
  each of these was found by a different library's tests being written against
  it. It claimed Java was the only one of the five that shows both columns, which
  stopped being true when Go, .NET, Python and Ruby each grew tests asserting the
  column they do not land on by default; the distinction that actually survives
  is narrower and is now stated as such — Java is the only one that reaches
  `writerShape` without naming a schema at all, because `registered(registry)` is
  handed none and the writer's schema comes off the wire and serves as the
  reader's. It said resolution in .NET is "always", which stopped being true when
  `WithoutReaderSchema()` and `Registered(registry, schema, readerSchema)`
  landed: it is always unless the caller declines it, and the opt-out is now
  named, both in the fixture and in the `## Schema resolution` table, whose .NET
  row said the same thing. The table's Python and Ruby rows said `Always` too,
  and were reported later because only .NET had been noticed: both resolve **by
  default**, `reader_schema=` and `reader_schema:` each move a codec off that
  default, and Ruby's fixed-schema `AvroCodec.of` has no registry to learn a
  writer schema from and so resolves nothing per message. All three rows now read
  as the fixture's corrected `why` lines do. And it explained Ruby's `resolved` column with "the
  codec is constructed with a schema", which is just as true of `AvroCodec.of` —
  a codec that resolves nothing, reads what it writes, and refuses
  `reader_schema:` with an `ArgumentError` — so the column is now attributed to
  `registered(...)` specifically. Go's and Python's lines were held to the same
  standard while the file was open, and the `writerShape` column definition
  widened to cover the reader holding the writer's own schema rather than none,
  which is how a library that always resolves arrives there at all. **The decoded
  values, the framed bytes, the schema ids and every column assignment are
  unchanged** — this is a prose-only regeneration, and the four copies need
  taking again.
- **Every cross-page link under `docs/` was written as `.html`, so every one of
  them was dead when the pages were read on GitHub.** That is where somebody
  meets these pages before they find the site — a repository is the first thing
  a link to it opens — and none of the 75 links worked there. The convention the
  other four libraries follow, and that this repository's own build script
  documents, is to write them as `.md` and let the build rewrite them for the
  rendered copy; here there was nothing for that rewrite to do, so it had been
  silently inert since it was written. All 75 are now `.md`, fragments intact
  (`reliability.md#replay`), and the one link that is genuinely HTML —
  `apidocs/index.html`, which javadoc generates and no markdown renders into —
  is deliberately left alone. The rendered site is byte-for-byte what it was.
  `build-docs-site.sh` now also checks the source side before it renders
  anything: every `.md` target must exist as a file in `docs/`, and a docs page
  linked as `.html` is named as the mistake it is. The existing check runs on the
  rewritten output and is satisfied by the `.html` file existing, so it could
  never have caught this; it is also, for the first time, checking a rewrite that
  actually fires.
- **Three sentences about past releases would have been rewritten into falsehoods
  by the release itself.** `set-documented-version.sh` replaces every
  three-segment `0.x.y` under `docs/` and in the README with the version being
  released, which is right for the coordinates it exists to keep current and
  wrong for a sentence about history: cutting this release would have turned
  "since 0.5.0 all five write both" into a claim about 0.6.0, and the stream
  segment-size argument and the replay-header rename would each have been
  re-dated to the release that did not introduce them. The script's comment
  asserted that every `0.x.y` in those files is a version somebody copies —
  checked when written, and no longer true once pages started referring to
  earlier releases. All three now use the two-segment form the rewrite
  deliberately does not match (`0.5`), which is what the surrounding prose
  already did ("up to and including 0.4"), and the script records the convention
  so the next such sentence is written safely rather than found afterwards.

## [0.5.0] - 2026-09-09

### Added
- **`parked` is a metric outcome, so a message nothing can read no longer looks
  like a dependency being down.** Java has always had a parking lot — a payload
  that will not decode goes to `{queue}.parked` rather than round the retry
  ladder — and has never had a word for it in `MetricNames`. It does now:
  `MetricNames.OUTCOME_PARKED`, reported on
  `acemq.messages.dead.lettered.total` alongside the `dead_lettered` that was
  always there, and as a `message.parked` event on the span. The two want
  different responses on call: dead-lettered is usually a dependency that will
  come back, parked is a deploy or a schema change that will not fix itself.
  A panel or an alert on `acemq.messages.dead.lettered.total` with no `outcome`
  selector still counts both and is unaffected. Go, .NET, Python and Ruby all
  have this word already.
- **`acemq.messages.set.aside.failed`, tagged `queue` and `target`, counts the
  republish to a dead-letter or parking queue failing.** Almost always a queue
  that was never declared. Nothing else in the estate could tell that apart from
  an ordinary dead-lettering — both look like one queue draining — and until now
  the only sign of it in Java was an exception on a path nobody watches. The
  message still goes back to the broker rather than being acknowledged, because
  counting is not a reason to change what happens to it. `MetricNames.TAG_TARGET`
  names the tag. Go, Python and Ruby raise the same counter in the same place,
  so one alert reads the same against all four.
- **`acemq.retry.rung.missing`, tagged `queue` and `rung`, counts a backoff that
  had to wait in the consumer because its rung queue is not on the broker.** The
  `rung` tag — `MetricNames.TAG_RUNG` — is the queue to declare, which is what
  makes the counter actionable rather than merely alarming. Nothing breaks —
  the message is still retried and the wait still happens — but the reason the
  rung exists is lost, because a consumer restarted mid-wait turns a five-minute
  backoff into no backoff at all. Java logged this and reported it nowhere; Go,
  Python and Ruby have counted it for some time.
- `Telemetry.messageParked`, `Telemetry.setAsideFailed` and
  `Telemetry.retryRungMissing`, all default no-ops, so a sink written before they
  existed keeps compiling and keeps working.
- **`Itinerary`: the routing slip that travels with the message, so a Java step
  can read a route a Go, Python or Ruby service wrote.** Java had one routing
  slip — `x-acemq-route`, a list of step names resolved against a declared
  `Pipeline` — and the other three had another: `acemq-routing-slip`, the whole
  itinerary as JSON, each stop naming its own exchange and routing key. Neither
  side could read the other's, so a polyglot route was not a route. Both work
  now. A pipeline step reads whichever the message carries, and a message
  carrying an itinerary is sent to the next stop **on the slip** rather than to
  the next step in the declaration — which is what lets a Java consumer sit in
  the middle of a route no Java service declared. `pipeline.send(itinerary,
  payload)` starts a run with one. The JSON is byte-identical to what Go and Ruby
  write, including omitting `done` until there is something in it, and the tests
  assert against literals those two libraries produced rather than against a
  reading of their source. **The declared form stays the default**: an ordinary
  `pipeline.send(payload)` writes exactly what it wrote before.
- **A stream can be declared with a segment size.** `mq.declareStream(name,
  maxAge, maxLengthBytes, segmentBytes)` sets
  `x-stream-max-segment-size-bytes`, which Go, Python and Ruby have exposed for
  some time and Java had no way to express at all — so a stream one of them
  declared could not be declared identically here, and the second declaration of
  it was refused. **No default is invented**: absent unless asked for, exactly as
  the other three behave, because a segment size Java added on its own would
  break every stream first declared elsewhere. Retention happens a whole segment
  at a time, so this is the granularity of every other retention setting.

### Changed
- **`acemq-replayed-at` is written as RFC 3339 rather than epoch milliseconds.**
  Go, Python and Ruby all wrote `2026-02-03T04:05:06Z` and Java wrote
  `1770091506789`, under the same header name, with nothing on the wire saying
  which — so a consumer reading that header had to know which library had
  produced the message. Java is one against three, so Java moved. **Reading is
  unchanged and still accepts every form**: the epoch milliseconds an older Java
  publisher wrote, the `Z` form Go and Ruby write, and the explicit `+00:00`
  offset Python writes — the last of which `Instant.parse` refuses on a Java 11
  runtime and which is now read as an offset date-time instead. **Anything
  comparing that header as a number needs to compare it as a timestamp.**
  `envelope-fixtures.json` changes on this one line; the other four repositories
  need the new copy.
- **Replay provenance moved out of the reserved header namespace, which changes
  the bytes on the wire.** A replay wrote `x-acemq-replayed-from`,
  `x-acemq-replayed-at` and `x-acemq-replay-count`; it now writes
  `acemq-replayed-from`, `acemq-replayed-at` and `acemq-replay-count`, which is
  what Go, Python and Ruby have always written. Java was the only library using
  the reserved prefix for these, and it was the wrong namespace for them twice
  over: `x-acemq-` is the engine's, and a header carrying it is stripped from a
  message's headers on the way in, so the one question the provenance exists to
  answer — *did this message come back off a dead-letter queue?* — could not be
  asked of the headers a handler was handed. They are ordinary application
  headers now and reach the handler as well as `Envelope.replayedFrom()`,
  `replayedAt()` and `replayCount()`, which are unchanged. **Anything matching on
  the old names — a shovel policy, a dashboard, a firehose consumer — needs the
  new ones.** `envelope-fixtures.json` now carries a `replayed` case, which is
  what would have caught this years earlier; the other four repositories need the
  new copy. `AceHeaders.SHARED_PREFIX` names this namespace, and reading
  `acemq-replayed-at` now accepts the ISO-8601 instant the Go, Python and Ruby
  replays write as well as the epoch milliseconds Java writes.
- **A handler's explicit rejection is now reported as `rejected` rather than
  `dead_lettered`, which changes what a dashboard shows.** A handler throwing
  `AceFatalException` and the engine exhausting a retry policy are different
  events — one is a decision somebody's code took about this message, the other
  is running out of room to try again — and reporting both as `dead_lettered`
  lost the distinction everywhere it mattered. The `outcome` tag on
  `acemq.consume.total` and the `messaging.acemq.outcome` attribute on the span
  both say `rejected` for the first case now, and the span still carries an
  `ERROR` status so nothing turns green on the way past. Where the message goes
  is unchanged: both still land in the dead-letter queue, and
  `acemq.messages.dead.lettered.total` and `MessageConsumer.deadLettered()` still
  count both, because those are about the destination rather than the reason.
  **An alert or a panel matching `outcome="dead_lettered"` to catch fatal handler
  failures needs `outcome="rejected"` too.** Go, Python and Ruby already drew the
  line here.

### Fixed
- **A Java requester and a Go, Python or Ruby responder could not talk to each
  other at all.** A request names the queue its answer goes to, and Java and .NET
  named it in AMQP's own `reply-to` property while Go, Python and Ruby named it in
  an `acemq-reply-to` header. Neither side read the other's, so a cross-language
  request went unanswered until the caller timed out, and the responder counted it
  as unanswerable — a request nobody could answer, which is exactly what it looked
  like from the inside. No fixture covered request/reply, which is why nothing
  caught it. A publisher told to expect a reply now writes **both**, always with
  the same value, and a responder reads **the header first and the property
  second** — the same order in all five libraries, so a new library and an old one
  interoperate in both directions. The header is preferred because it survives a
  hop that rebuilds the message, such as a retry rung or a shovel, where the
  property does not.
- **`JsonCodec` refused `text/json`.** A legacy alias, never correct to write and
  written all the same by older .NET stacks and a good deal of PHP. Go, Python and
  Ruby accept it, so a message they read happily was a poison message to a Java
  consumer beside them. The read set is now `application/json*`, `text/json*` and
  any `application/*+json`. The write side is unchanged and still
  `application/json`.
- **A span whose operation failed carried no outcome, while the counter for the
  same operation said `failed`.** `MicrometerTelemetry` has always defaulted a
  scope's `outcome` tag to `failed` when nothing named one, so the metric side
  of a thrown publish or a failed request was tagged. The OpenTelemetry side
  recorded the exception and an `ERROR` status and set no
  `messaging.acemq.outcome` at all — the one attribute a trace backend is
  queried on. A dashboard therefore showed failures that no trace search could
  find. `Telemetry.Scope.failed` now sets `messaging.acemq.outcome=failed` when
  no outcome has been named, and a scope closed without saying how it went is
  read the same way the meter scope has always read that silence. An outcome
  already named wins: a request that timed out and then unwound still reports
  `timed_out`, which says more than "it threw".
- **A fixed-schema `AvroCodec` refused legitimate messages whose first field
  encodes to a zero byte.** The check that stops a fixed-schema codec silently
  misreading Confluent-framed bytes was made on the bytes alone: five bytes or
  more beginning with `0x00`, refused. But an Avro body begins with `0x00`
  whenever its first field encodes to zero — an empty string, a `0`, a `false`,
  the first branch of a union — so an ordinary record was rejected as poison,
  with no way to read it at all. The content type now decides: told `avro/binary`
  (or `application/avro`, or any `*+avro`) the body is decoded as written, told
  `application/vnd.acemq.avro` it is still refused as the other framing, and the
  leading-byte guess survives only for a message that arrived saying nothing
  useful, where it is the sole signal there is. `AvroCodec` now overrides
  `decode(byte[], Class, String)`, which the engine already called; the
  two-argument form behaves as before. Python and Ruby agree.
- **`ProtobufCodec` refused `application/vnd.google.protobuf`**, which is what
  Google's own tooling and most schema registries write. A message the Go and
  Ruby libraries read happily was a poison message to a Java consumer beside
  them. The read set is now `application/x-protobuf`, `application/protobuf`,
  `application/vnd.google.protobuf` and any `*+protobuf` suffix type. What the
  codec writes is unchanged: `application/x-protobuf`.

## [0.4.0] - 2026-09-08

> ### ⚠ Migrating: a retry policy no longer invents an age limit
>
> `RetryPolicy` used to set `maxMessageAge` to 365 days in every factory —
> `exponential(...)`, `fixed(...)`, `none()` and the full constructor — and
> `nextWait` compared against it unconditionally. **A message that reached a
> year old was dead-lettered by a Java consumer and retried by the Go, .NET,
> Python and Ruby ones**, which have always read an age limit of zero as no
> limit at all. Zero is now the default here too, and means the same thing.
>
> If you were relying on the year, those messages are now retried until the
> attempts run out instead of being dead-lettered. Say it out loud to get the
> old behaviour back exactly:
>
> ```java
> RetryPolicy.exponential(5, ofSeconds(1), ofHours(24))
>         .giveUpAfter(Duration.ofDays(365));
> ```
>
> `giveUpAfter(...)` is unchanged and is now the only way to get an age limit.
> Its boundary is unchanged too: a message whose age is exactly the limit is
> abandoned, which is what all five libraries already did.

### Changed
- **`RetryPolicy`: zero means no age limit, and zero is the default.** The
  cross-language conformance suite found this on its first run — four libraries
  reading zero as never, and Java alone carrying a 365-day limit that no caller
  had asked for. A sentinel that stands in for "no limit" only works if nobody
  compares against it, and Java did, so the sentinel had quietly become a
  policy: an age at which to stop retrying, chosen by the library rather than
  by the caller. `maxAttempts` already bounds the retrying; the age limit is
  now off until `giveUpAfter(...)` turns it on. A negative limit reads as no
  limit rather than as abandon-everything, for the same reason.

  `contract-fixtures.json` moves with it. Each entry of `retrySchedules` now
  carries `hasMaxMessageAge` alongside `maxMessageAgeMillis`, so a port cannot
  mistake a zero for "abandon on first failure", and there is a new
  `maxMessageAge` section spelling out the rule with a computed table either
  side of the boundary — including the year-old message that used to be the
  disagreement. The four ports need the new bytes.

### Added
- **The cross-language conformance suite.** The generator that writes
  `envelope-fixtures.json` lived in the .NET repository and was run by hand
  with `javac`; it now lives in this repository's test suite, runs on every
  build, and fails the build when what it produces no longer matches what is
  committed. Alongside it is a second fixture, `contract-fixtures.json`, which
  pins the retry schedules, the jitter bounds, the consumer/broker threshold,
  the queue naming, the rung argument table, the full declared topology and the
  queue type defaults.

  This is the answer to a bug that survived ten releases. `RetryPolicy.exponential`
  multiplied by five and jittered by ten percent in Java while Go, .NET, Python
  and Ruby doubled and jittered by twenty, so `exponential(5, 1s, 1m)` produced
  `1s, 5s, 25s, 60s` here and `1s, 2s, 4s, 8s` everywhere else. Three further
  divergences turned up the same week, all four found by a person reading five
  codebases side by side. That does not scale. A schedule that changes now turns
  this repository's build red on the commit that changed it.

  Nothing in the published API moved. Both fixtures and the notes on how to
  regenerate them are in `acemq-amqp-test/src/test/resources/fixtures/`.

## [0.3.0] - 2026-09-08

> ### ⚠ Migrating: `queueWithDeadLetter` changes a queue's arguments
>
> `Topology.Builder.queueWithDeadLetter(...)` and
> `classicQueueWithDeadLetter(...)` declare the source queue with
> `x-dead-letter-exchange` and `x-dead-letter-routing-key` on it. **A queue that
> already exists without those arguments cannot be redeclared with them.** AMQP
> forbids changing a queue's arguments in place, so the declare is refused with
> `PRECONDITION_FAILED` and, on AMQP 0-9-1, the refusal closes the channel.
>
> Nothing breaks by upgrading. These are new methods; `queue(...)` and
> `classicQueue(...)` declare exactly what they declared before, and a service
> that does not call the new ones sees no change. The break happens when you
> **switch an existing queue over to them**, and it happens at start-up on the
> day of the deployment.
>
> Before switching `orders.new` over, do one of these:
>
> - **Drain and recreate it.** Stop the consumers, let the queue empty, delete
>   it, and let the new topology declare it. The only option that needs no
>   coordination, and it costs whatever downtime draining takes.
> - **Migrate it.** Declare `orders.new.v2` with the new arguments, move the
>   messages across (`Replay`, or the shovel plugin), swap the bindings, delete
>   the old queue. No downtime, more steps.
> - **Declare it yourself and keep `queue(...)`.** Add the two arguments to your
>   own provisioning — `x-dead-letter-exchange` is `acemq.dlx` and
>   `x-dead-letter-routing-key` is `{queue}.dlq` — and declare `{queue}.dlq` and
>   `{queue}.parked` bound to `acemq.dlx` on their own names. Correct for an
>   estate where topology is provisioned outside the application.
>
> Run `mq.topology().plan(...)` first either way. It reports the difference as
> drift, in the broker's own words, without touching anything.

### Added
- **The source queue now says where its dead letters go.**
  `Topology.Builder.queueWithDeadLetter(name)` and
  `classicQueueWithDeadLetter(name, arguments)` declare a queue together with
  `{name}.dlq`, `{name}.parked` and the `acemq.dlx` exchange that reaches them,
  and — the part that was missing — put `x-dead-letter-exchange` and
  `x-dead-letter-routing-key` on the source queue itself.

  Java bound the two dead-letter queues correctly and never stamped the queue
  they were for, which is the one argument table two services have to agree on.
  A Python service declaring `orders` with those arguments and a Java service
  declaring it without them cannot both consume it: the second one to start is
  answered `PRECONDITION_FAILED` and consumes nothing. The values are the ones
  Go, .NET, Python and Ruby already send, so this is Java catching up rather
  than a fifth convention.

  This is a backstop and not a replacement for the dead-lettering the consumer
  already does. A handler that fails is still republished to `{queue}.dlq` with
  the reason recorded on the envelope and then acknowledged, because the
  broker's own `x-death` header records that a message died and not why. What
  the broker route adds is everything the library never sees: a message expiring
  under the queue's `x-message-ttl`, an overflow under `x-max-length`, a reject
  from some other consumer of the same queue. Those used to vanish. Both paths
  stay.

  A caller who has already set either argument by hand is refused rather than
  overruled. Silently replacing an instruction someone wrote down would send
  their messages somewhere else with nothing to say so.
- **A threshold between waiting here and waiting there.**
  `RetryPolicy.brokerWaitThreshold()` is thirty seconds by default, and
  `waitInBrokerFrom(...)` moves it — zero meaning never, for a service that is
  not allowed to declare queues on its broker. A wait shorter than the threshold
  is spent in the consumer; a wait at or above it is published into a rung queue
  as before. `nextWait(...)` reports both halves of that answer, because they
  cannot be worked out separately, and `brokerRungs()` is the list of delays that
  actually need a queue.

  Thirty seconds is where the two costs cross. Below it a held prefetch slot is
  cheaper than a queue nobody asked for, and the seconds a restart loses are only
  seconds. Above it a consumer that restarts mid-wait loses the wait entirely,
  because the broker redelivers the unacknowledged message at once — which is a
  correctness bug rather than a throughput one.

  The same threshold and the same default are in the Go, .NET, Python and Ruby
  libraries, so the same policy needs the same rungs whichever one declares the
  topology.
- `Replay.keepingAttempts()`, for putting a message back exactly as it was. The
  default still resets the attempt counter, because a message dead-lettered on
  the last attempt of a five-attempt policy would otherwise be dead-lettered
  again before any handler saw it.

### Changed
- The binary-compatibility gate now compares against a version that exists. It
  was configured with `ignoreMissingOldVersion` and no baseline, so japicmp
  resolved nothing, reported "Ignoring missing old artifact version" for every
  module, and passed — a gate that had never compared anything. It now names
  `0.2.10`, resolved from the published repository, which is declared in the
  build for that one purpose. `ignoreMissingOldVersion` stays on for modules
  added since that release, which have no old artifact to compare against.
- The project version was `0.2.8-SNAPSHOT` while `0.2.10` was released and
  tagged, so the working tree claimed to be older than the last two releases.
  It is now `0.2.11-SNAPSHOT`.
- **`RetryPolicy.exponential(...)` now doubles rather than multiplying by five,
  and jitters by twenty percent rather than ten.** `exponential(5, 1s, 1m)` was
  1s, 5s, 25s, 60s and is now 1s, 2s, 4s, 8s. The numbers a policy produces are
  part of the cross-language contract: the same message can be retried by a
  consumer written in any of the five languages, and a message that waited one
  second under one library and five under another has no schedule at all. The
  four-argument overload still takes an explicit multiplier for anyone who wants
  the old growth.
- A retry ladder is now built from the delays at or above the threshold only, so
  a schedule that runs in a few seconds declares no rung queues at all. Rungs are
  also deduplicated by queue *name* as well as by delay: two delays that render
  to the same name are one queue, and declaring it twice with different
  time-to-live values is a `PRECONDITION_FAILED` rather than a second rung.
- Jitter is no longer applied to a wait the broker will hold. A rung's
  time-to-live is fixed when the queue is declared, so a jittered delay named a
  queue that did not exist; and the spread is already there up at that scale,
  because each message's time-to-live starts when it arrives on the rung.

## [0.2.10] - 2026-09-02

### Added
- **Claim check** — `ClaimCheckCodec` plus in-memory and filesystem stores.
  Payloads above a threshold go to a store and the message carries the key;
  below it they travel inline, because offloading a small message turns one
  round trip into two. The framing says which it is, so a consumer reads both
  without being told and the threshold can change without a flag day. Messages
  written before the codec existed are still readable. `keyOf(body)` answers
  "which object does this need" from a dead-letter queue without fetching it.
- **Scheduling** — `Scheduler.in(...)` and `.at(...)`.

  **Not a per-message time to live.** The obvious implementation is wrong for
  mixed delays: a classic queue expires messages only at its head, so a four-hour
  message followed by a one-minute message delivers the second in four hours,
  and nothing reports it. Instead a ladder of queues each with a *uniform* TTL,
  with messages hopping until due — every message in a rung has the same delay,
  so the head is always the one due soonest.

  The cost is stated rather than hidden: long delays are several round trips, and
  accuracy is about the smallest rung.
- **Saga** — `Saga` and `SagaResult`. Steps that each know how to undo
  themselves, compensating in reverse when one fails.

  Deliberately not sold as a distributed transaction: after a payment step the
  money really has moved, and the refund is a new fact rather than an erasure.
  Deliberately not durable either — state is on the stack, so a crash midway
  leaves it half-applied.

  When a compensation itself fails it is logged and the remaining ones still
  run, because stopping leaves more undone. `unresolved()` is the list of effects
  that happened, were meant to be undone, and were not — the thing to alert on,
  because no retry resolves it.

  All three were advertised in the README for months before they existed. They
  were built after three applications had needed them, which is why the shapes
  are what they are.

### Changed
- `AceHeaders.PREFIX` now documents that `x-acemq-` is **reserved**: a header
  carrying it is dropped from the application's view on the way in, because
  engine headers are materialised as envelope fields instead. Using it for your
  own header means writing it on publish and finding it gone on consume, with
  nothing reporting the loss. Found by doing exactly that while writing the
  scheduler.
- The getting-started page and tutorial 1 showed only
  `declareExchange`/`declareQueue`/`bind`, which is what every AMQP client
  offers, and never mentioned the `Topology` builder. Both now follow the three
  calls with the value form and what it buys: a printable plan, `VALIDATE`, and
  drift caught before anything is declared. The imperative calls stay where they
  are teaching what the three things are.

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
