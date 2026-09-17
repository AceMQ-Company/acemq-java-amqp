# The envelope

What travels with a message besides its body: identity, causation, provenance and
the counters the retry engine keeps. It is written and read by the library rather
than by your code, and it is the part that crosses languages — a Java publisher
writes these headers and a Go, Python, Ruby or .NET consumer reads them back
unchanged, or the shared fixtures fail.

```java
Envelope e = message.envelope();

e.id();             // String  — unique, and the default idempotency key
e.type();           // String  — the logical type, such as order.placed
e.version();        // int     — schema version of the payload, at least 1
e.correlationId();  // String  — the whole flow
e.causationId();    // Optional<String> — the immediate parent
e.attempt();        // int     — delivery attempt, from 1
e.firstSeen();      // Instant — when it was first published
e.origin();         // Optional<String> — the publishing process
e.error();          // Optional<String> — why it was set aside
e.claim();          // Optional<String> — where the payload is, if stored outside
e.route();          // Optional<RoutingSlip>
e.replayedFrom();   // Optional<String>
e.replayedAt();     // Optional<Instant>
e.replayCount();    // int
e.headers();        // Map<String, Object> — yours, and only yours
```

An envelope is **immutable**. The engine derives a new one rather than changing
one in place, which is what makes an attempt counter trustworthy when the same
message is delivered several times: the envelope a handler was given still says
what it said after the retry path has built the next one. The optional accessors
are optional for a reason — an absent value is an absent header, never a null
one, and the type says so before the wire does.

It would be a `record`. The published bytecode targets Java 11 so Spring Boot 2.7
applications can consume the library; see ADR-015.

## On the wire

| Header | Type | |
|---|---|---|
| `x-acemq-id` | string | The identifier. Always written. |
| `x-acemq-type` | string | The logical type. Always written. |
| `x-acemq-version` | **integer** | Schema version, from 1. Always written. |
| `x-acemq-correlation` | string | Propagated unchanged across hops. Always written. |
| `x-acemq-attempt` | **integer** | Delivery attempt, from 1. Always written. |
| `x-acemq-first-seen` | **integer** | Epoch **milliseconds** of the first publish. Always written. |
| `x-acemq-causation` | string | Omitted when there is no causation. |
| `x-acemq-origin` | string | Omitted when unknown. |
| `x-acemq-error` | string | Why the engine set the message aside. Present only on a message read back from a dead-letter or parking queue. |
| `x-acemq-claim` | string | Where the payload is, when the application stores it outside the message. Omitted when unset. |
| `x-acemq-route` | string | Step names of a declared pipeline, comma-joined. |
| `x-acemq-route-position` | **integer** | Which of them this message is for, from 0. |
| `x-acemq-route-id` | string | One run through that route, across every hop. |
| `acemq-replayed-from` | string | Queue it was replayed out of. |
| `acemq-replayed-at` | **string** | RFC 3339 instant, when last replayed. |
| `acemq-replay-count` | integer | How many times. Omitted when zero. |

Values are written as strings and integers only, because those are the two types
every AMQP client's mapping carries intact.

**The two timestamps are encoded differently and that is the contract.**
`x-acemq-first-seen` is an integer of epoch milliseconds; `acemq-replayed-at` is
an RFC 3339 string — `2026-02-03T04:05:06.789Z`. Tidying either one up in a port
produces messages the other four libraries misread.

`x-acemq-claim` is **set by the application, never by the engine**. It is there
for an application that wants an operator reading a dead-letter queue to see
where a payload went — conventionally a URI:

```java
mq.publisher("orders", "order.placed", Order.class)
  .send(order, Envelope.of("order.placed")
                       .claim("s3://payloads/orders/o-1")
                       .build());
```

It is an envelope field rather than an ordinary header because `x-acemq-` is the
engine's namespace: left as a header it would be stripped on the way in and never
reach the handler. Absent rather than empty — a `null` or `""` claim writes no
header, the same as the other optional fields, so nothing at the other end has to
special-case a header carrying nothing.

**It is not the claim-check pattern.** The
[claim check](patterns.md#the-claim-check) frames its reference in the *body*
and sets no header at all, deliberately: a header can be stripped by a shovel or
a federation link, and a present-or-absent header cannot say whether a payload
travelled inline. All five libraries make that choice, and this field does not
change it.

The reply address is not in that table either. It travels as
`acemq-reply-to` *and* as AMQP's own `reply-to` property, both written and either
read — see [request and reply](request-reply.md#how-it-works).

## The defaults are contract, not convenience

| When you do not set it | It becomes |
|---|---|
| `id` | a random UUID v4 |
| `type` | the **routing key** |
| `correlationId` | the **message id** |
| `origin` | `{clientName}@{hostname}`, and `clientName` is `acemq` unless you set it |
| `version`, `attempt` | `1` |
| `firstSeen` | now |
| `causationId` | **absent** — the header is omitted, never written as null |

The AMQP `messageId` property also mirrors `x-acemq-id`, and is read back as the
identifier when the header is missing — which is how a message from a service
that never heard of AceMQ still gets one.

Two of those have edges worth knowing. `origin` is
`ConnectionConfig.clientName() + "@" + hostname`, so `acemq@app-7` is what a
default connection stamps and `checkout@app-7` is what one named `checkout`
stamps; a host the JVM cannot resolve becomes `unknown-host` rather than failing
the publish. And `type` falls back to the routing key **at both ends** — at
publish when you call `send(payload)` without an envelope, and at consume when a
message arrives with no type header — except that a routing key of `""`, which is
what a fanout publish has, yields the literal type `message`. There is no such
thing as an empty type here.

Defaults are applied when the envelope is **built**, not when it is read, so two
libraries reading the same message agree without needing a second set of rules to
agree on. `version` and `attempt` below 1 are refused outright rather than
corrected: a caller who asked for attempt 0 has a bug, and the builder is where it
is cheapest to find.

## Two namespaces, and why there are two

**`x-acemq-` is reserved.** A header carrying that prefix belongs to the engine.
On the way in it is materialised onto the envelope if this version knows it, and
**dropped from `headers()` either way** — including names this version has never
heard of, so a header written by a newer release of another language's library
cannot masquerade as one the application set. On the way out,
`Envelope.Builder.header` refuses it:

```java
Envelope.of("order.placed").header("x-acemq-id", "mine");
// IllegalArgumentException: header 'x-acemq-id' is owned by AceMQ and is
// derived from the envelope, so it cannot be set directly
```

Use a namespace of your own — `x-yourcompany-` — for anything that has to survive
the round trip. `AceHeaders.isAceHeader(name)` is the whole test, and it tests the
prefix rather than a list, so a field added to the contract next year does not
start appearing in application code that predates it.

**`acemq-` is defined but not reserved.** `AceHeaders.SHARED_PREFIX` names it, and
`isAceHeader` deliberately does not match it. A header here is one AceMQ writes
and reads, but it is an ordinary application header on the wire: the builder
accepts it, and it reaches the handler like any other. That is the point. A
responder has to read the reply address out of the message it was handed, and a
handler has to be able to see that the message it is looking at came back off a
dead-letter queue. Put either in the reserved namespace and the engine eats it on
the way in. `acemq-reply-to`, `acemq-routing-slip` and the three replay stamps
live here.

**The replay three changed spelling in 0.5.0.** Up to and including 0.4 this
library wrote `x-acemq-replayed-from`, `x-acemq-replayed-at` and
`x-acemq-replay-count` — the reserved namespace, stripped on consume — so the one
question replay provenance exists to answer, *did this message come back off a
dead-letter queue?*, could not be asked of the headers a handler was given. Java
was the only library doing it. **Anything matching on the old names — a shovel
policy, a dashboard, a firehose consumer — needs the new ones.** The values now
arrive twice, as `replayedFrom()`, `replayedAt()` and `replayCount()` on the
envelope *and* as ordinary entries in `headers()`; that duplication is deliberate,
and writing puts the fields over the top of the headers rather than around them so
the two cannot say different things about one message. See
[replay](reliability.md#replay).

W3C trace context — `traceparent` and `tracestate` — is unprefixed for the same
reason plus one more: other tools already know those names. It reaches handlers as
ordinary headers. See [traces cross the broker](observability.md#traces-cross-the-broker).

## The route

`x-acemq-route`, `x-acemq-route-position` and `x-acemq-route-id` are reserved, and
so they are read onto the envelope as a `RoutingSlip` rather than left among your
headers — a step that could not read where it was going would be no step at all.
`envelope.route()` gives it; `steps()`, `position()`, `current()`, `next()` and
`runId()` read it. A message dead-lettered at step two keeps its slip, which is
what makes replaying it *resume* the run instead of starting it again.

That trio is this library's declared-pipeline form. The other form, the
self-describing `acemq-routing-slip` JSON that Go, Python and Ruby write, is an
ordinary header in the shared namespace and is read by `Itinerary`. Both are read;
see [pipelines](patterns.md#pipelines).

## Reading it in a handler

```java
mq.consume("orders.new", Order.class, message -> {
    Envelope e = message.envelope();
    log.info("{} type={} attempt={} correlation={} from={} age={}",
            e.id(), e.type(), e.attempt(), e.correlationId(),
            e.origin().orElse("unknown"), e.age());
    Object tenant = message.headers().get("x-tenant");   // == e.headers()
});
```

`age()` is computed from `firstSeen()` rather than stored, and it is the figure a
retry policy uses to give up on a message too old to be worth delivering. A
message can be on attempt two and four days old — that is what a paused queue
looks like — so age is the honest limit where an attempt count is not.

`message.headers()` returns `envelope().headers()`; there is one map, not two.

## Building one

```java
Envelope envelope = Envelope.of("order.placed")
        .version(2)
        .correlationId(incoming.envelope().correlationId())
        .causationId(incoming.envelope().id())
        .header("x-tenant", "acme")
        .build();

publisher.send(order, envelope);
```

Most code never does this: `send(payload)` builds one. When you do build one,
three helpers cover nearly every case.

`causing(type)` returns a builder for a message caused by this one — correlation
carried across, causation set to this message's id, attempt restarted, origin
inherited. `nextAttempt()` returns a copy with the counter advanced and everything
else, `firstSeen` included, preserved. `toBuilder()` copies the lot, which is what
an [interceptor](publishing.md#cross-cutting-concerns) uses to stamp something
onto every outgoing envelope.

Note the asymmetry between the two header methods, because it is easy to trip
over: `header(name, value)` **throws** on a reserved name, while
`headers(map)` **silently skips** them. The map form exists to copy an incoming
message's headers onto an outgoing one, where reserved names are expected to be
present and expected not to be propagated; the single-name form exists because you
typed it deliberately.

Provenance is stamped by the engine at publish, not by the caller — an application
building its own envelope to carry correlation should not also have to know its own
hostname. An `origin` you did set is left alone.

## Reading values off the wire

Broker clients are not consistent about header types. RabbitMQ's Java client hands
back `LongString` rather than `String` for anything long, and it is not a
`CharSequence`, so everything textual is read through `toString()`; an integer may
arrive as any width, or as a string. All of it is coerced rather than one shape
being assumed.

Reading is deliberately forgiving in one direction only. Anything **missing**
takes its default, and anything **unreadable** takes its default too: a producer
that wrote `x-acemq-attempt` as the string `"2"` still sent a message, and
refusing to deliver it would hand the application an outage rather than a message.
A message with no AceMQ headers at all is still readable — a first attempt of a
type named after its routing key — which is what lets AceMQ consumers be
introduced to an existing system one service at a time.

An **empty string reads as absent**. A producer that wrote `x-acemq-origin: ""`
gets `origin()` empty rather than an `Optional` holding nothing useful.

`acemq-replayed-at` is read in three forms: the epoch milliseconds an older Java
publisher wrote, the `Z` instant Go and Ruby write, and the explicit `+00:00`
offset Python writes — the last of which `Instant.parse` refuses on a Java 11
runtime and which is parsed as an offset date-time instead. Only RFC 3339 is ever
written.

## How this is kept honest

`acemq-amqp-test/src/test/resources/fixtures/envelope-fixtures.json` is
**generated**, never written by hand. The generator publishes real messages
through this library and pulls them back at the transport level — the only place
the engine's own headers are still visible, since the consumer API strips them by
design — and `FixtureDriftTest` fails when the committed file differs from what
the library puts on the wire today, masking only the clock and the hostname.

Go, Python, Ruby and .NET each carry a byte-identical copy and assert against it,
so a change to the wire contract here is a change in five repositories and CI says
so on the commit that caused it. That is the difference between a port and a
claim: a contract hand-copied out of documentation acquires a difference nobody
notices until two languages disagree in production, at which point the message
that proves it is the one already in the dead-letter queue.

## Related

- [Publishing](publishing.md) — setting the fields, and interceptors that stamp them
- [Consuming](consuming.md) — where a handler meets one
- [Reliability](reliability.md#replay) — `attempt`, `error` and the replay stamps
- [Patterns](patterns.md#pipelines) — the two routing slips
- [Observability](observability.md#traces-cross-the-broker) — trace context beside the envelope
