# Broker compatibility

This library is developed against RabbitMQ 4.x. A large part of the installed
base is still on 3.13, so "does it work on 3.13" needs an answer that came from
a run rather than from release notes.

**It does.** The whole integration suite passes unmodified on both, and nothing
in this library is gated on a broker version — there is no version detection
anywhere in it, so nothing silently degrades. Every row below was produced by
running against a real broker of each version; nothing here is transcribed from
a changelog.

| | |
|---|---|
| Verified against | RabbitMQ **3.13.7** (Erlang 26.2.5) and **4.3.6** (Erlang 27.3.4) |
| Minimum supported | **RabbitMQ 3.13** |
| Developed and released against | RabbitMQ 4.x |
| Re-checked by | the `compatibility` job in `ci.yml`, on every pull request |

## What was run

The `acemq-transport-rabbitmq` integration suite — 53 tests across publishing
with confirms, asynchronous and batch publishing, consuming, the retry ladder,
dead-letter and parking queues, replay, streams, request and reply, publish
options, blocked connections, topology drift and cluster failover — against each
broker in turn:

```
mvn verify -pl acemq-transport-rabbitmq \
    -Dacemq.test.rabbitmq.image=rabbitmq:3.13-management
```

| Broker | Tests | Failures | Errors | Skipped |
|---|---|---|---|---|
| RabbitMQ 4.3.6 | 53 | 0 | 0 | 2 |
| RabbitMQ 3.13.7 | 53 | 0 | 0 | 2 |

The two skipped are `LargeClusterIT`, which the nightly job starts with an
explicit cluster size and an ordinary run does not.

`-Dacemq.test.rabbitmq.image` is the supported way to point the suite at any
broker image, so this is reproducible against whatever version an estate
actually runs.

## The feature matrix

Everything the library offers works on both versions. There is no feature here
that needs 4.x.

| Feature | 3.13 | 4.x | Notes |
|---|---|---|---|
| Publishing with confirms | Yes | Yes | |
| `sendAsync` and `sendAll` | Yes | Yes | |
| Unroutable-message detection | Yes | Yes | `mandatory` plus a return listener; identical on both |
| Consuming, acknowledgement, prefetch | Yes | Yes | Per-consumer QoS only — see [global QoS](#global-qos-is-denied-in-4-and-this-library-never-asked-for-it) |
| Classic queues | Yes | Yes | Always declared durable |
| Quorum queues | Yes | Yes | The default for `declareQueue`; needs 3.8+ |
| Streams | Yes | Yes | Needs 3.9+ |
| Stream segment size | Yes | Yes | `x-stream-max-segment-size-bytes`, added by this library in 0.5 |
| Retry ladder | Yes | Yes | Built from `x-message-ttl` and `x-dead-letter-exchange` |
| Dead-letter and parking queues | Yes | Yes | |
| Replay | Yes | Yes | |
| Request and reply | Yes | Yes | |
| Priority queues | Yes | Yes | `x-max-priority` |
| Message TTL and queue expiry | Yes | Yes | `x-message-ttl`, `x-expires` |
| Topology drift detection | Yes | Yes | See [drift is immune to the `x-queue-type` change](#drift-detection-is-not-affected-by-the-x-queue-type-change) |
| Cluster failover and recovery | Yes | Yes | |
| TLS (`amqps://`) | Yes | Yes | |
| Payload encryption | Yes | Yes | Client-side; the broker is not involved |
| All six serialization formats | Yes | Yes | Client-side; the broker is not involved |

## Where the two brokers actually differ

These are the differences that exist. None of them changes what this library
does, and the reason each one does not is worth stating, because "it happens to
pass" and "it cannot break" are different claims.

### Transient non-exclusive queues are denied in 4

The clearest behavioural break between the versions, and the library cannot
reach it.

| | 3.13.7 | 4.3.6 |
|---|---|---|
| `queue.declare` durable=false, exclusive=false | Accepted | **Refused** |
| Deprecation phase | `permitted_by_default` | `denied_by_default` |

On 4 the refusal closes the **connection**, not just the channel —
`541 INTERNAL_ERROR`, `Feature 'transient_nonexcl_queues' is deprecated` — which
is a harsher failure than a channel-level `PRECONDITION_FAILED` and worth
knowing about if you meet it.

**This library never declares one.** Every queue and exchange it declares is
durable: `durable` is hard-coded `true` on every builder path in `Topology`, and
`AceMq.declareQueue` has no durability parameter to pass. Quorum and stream
queues are forced durable regardless, because the broker requires it.

So this deprecation cannot bite through AceMQ. It can still bite the estate
around it — anything else declaring transient queues on 3.13 will fail on the
connection after an upgrade, and that is a migration item for the operator
rather than for this library.

### Classic queue mirroring is gone in 4

| | 3.13.7 | 4.3.6 |
|---|---|---|
| `PUT /api/policies` with `ha-mode` | `201 Created` | **`400 Bad Request`** — "not recognised policy settings" |
| Deprecation phase | `permitted_by_default` | `removed` |

`ram_node_type` is likewise `removed` in 4, and `queue_master_locator` is
`denied_by_default`.

**This library sets none of them.** It writes no policies at all — policies are
an operator's tool, applied out of band — and the only queue arguments it ever
sends are `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-expires`,
`x-max-age`, `x-max-length-bytes`, `x-max-priority`, `x-message-ttl`,
`x-queue-type` and `x-stream-max-segment-size-bytes`. All nine are accepted by
both versions.

If a 3.13 estate depends on mirrored classic queues for availability, that
availability has to be re-established with **quorum queues** before the upgrade.
`declareQueue` already defaults to quorum, so an AceMQ application is on the
right side of that migration without changing anything.

### Drift detection is not affected by the `x-queue-type` change

RabbitMQ 4 records `x-queue-type` on every queue; 3.13 leaves it off a classic
queue. Declaring the same classic queue through this library and then reading it
back over the management API:

| | 3.13.7 | 4.3.6 |
|---|---|---|
| `arguments` reported for a classic queue | `{}` | `{"x-queue-type": "classic"}` |

That difference is real, and it is exactly the trap that makes an
argument-equality check report drift on every classic queue on 4 and none on
3.13.

**[Drift detection here](topology.md#how-it-is-detected) does not compare
argument maps.** It re-declares the queue with the arguments the plan asks for
and reads the broker's `406 PRECONDITION_FAILED` reply code, so the broker
itself decides equivalence and the answer is the same on both versions.
Re-declaring a classic queue with an explicit `x-queue-type: classic` against
one originally declared without it is equivalent on both — confirmed by running
it.

The trap is real for anything that *does* read `/api/queues` and compare
arguments; `acemq-java-rabbitmq-admin` documents it for that reason. It does not
apply to the message path.

### Global QoS is denied in 4, and this library never asked for it

| | 3.13.7 | 4.3.6 |
|---|---|---|
| `global_qos` deprecation phase | `permitted_by_default` | `denied_by_default` |

`basic.qos` with `global=true` sets one prefetch across a whole channel rather
than per consumer. This library calls the single-argument
`channel.basicQos(prefetch)`, which is `global=false`, everywhere it sets a
prefetch — on subscribe and on a later
[`prefetch(int)`](consuming.md#prefetch) change. Nothing to do.

### The stream prefetch refusal reads differently

Both versions refuse a stream consumer with no prefetch, which is what
[`StreamOptions`](streams.md#prefetch) exists to prevent. The message differs:

| | Text |
|---|---|
| 3.13.7 | `PRECONDITION_FAILED - consumer prefetch count is not set for 'queue 'x' in vhost '/''` |
| 4.3.6 | `PRECONDITION_FAILED - consumer prefetch count is not set for stream queue 'x' in vhost '/'` |

**This library matches on the numeric reply code, never on the text**, so the
wording is free to differ. It is listed because anything of your own that
string-matches broker errors will break across this upgrade, and that is a
common enough shortcut to be worth naming.

### Health-check responses differ in shape

Not this library's surface — it exposes no HTTP health check, and
`protocol-listener` belongs to the management API — but it is the difference
most likely to break a deployment's readiness probe during an upgrade.

| `GET /api/health/checks/protocol-listener/{p}` | 3.13.7 | 4.3.6 |
|---|---|---|
| Known protocol | `200` `{"status":"ok","protocol":"amqp"}` | `200` `{"status":"ok","protocols":["amqp",…]}` |
| Unknown protocol | `503`, `"missing":"nosuchproto"` | `503`, `"missing":["nosuchproto"]` |

Both answer `503` for a missing listener, so a probe that only reads the status
code is unaffected. One that reads `protocol` or treats `missing` as a string
breaks on 4.

## What the library does when it meets the older broker

Nothing special, and that is deliberate.

There is **no broker-version detection in this library.** The RabbitMQ transport
reports a fixed [capability set](testing.md) and never queries the server's
version to decide what to offer. The consequences are worth being explicit
about:

- **Nothing degrades silently.** There is no code path that notices an older
  broker and quietly substitutes something weaker. A feature either works or the
  broker refuses the operation and the refusal is raised as a
  `TransportException` naming what the broker said.
- **Nothing is withheld on 3.13.** Every capability the library claims is
  claimed on both versions, because every one of them is genuinely available on
  both.
- **A refusal is the broker's, reported verbatim.** When a broker does refuse —
  an argument it does not know, a queue type it does not have — the reply code
  and the broker's own text are what you see, rather than a message this library
  invented about what it guessed the version was.

The one asymmetry worth planning around is not a library behaviour at all: a
3.13 broker will accept things a 4.x broker refuses, so **a topology that works
on 3.13 is not automatically one that works on 4.** Transient queues and
`ha-mode` policies are the two that matter, and neither comes from here.

## Minimum versions by feature

| Feature | Needs |
|---|---|
| Everything on the message path | RabbitMQ 3.13 |
| Quorum queues | 3.8 |
| Streams, stream offsets and retention | 3.9 |
| Stream segment size | 3.9 |
| Single Active Consumer on a stream | 3.11 |

3.13 is the floor this library tests and supports, so the older numbers are
context for an estate mid-upgrade rather than a promise that 3.8 is supported.

## Keeping this honest

A matrix nothing re-checks goes stale exactly the way the claim it replaced did.
The `compatibility` job in `.github/workflows/ci.yml` runs the integration suite
against `rabbitmq:3.13-management` on every pull request, separately from the
JDK build matrix — the JDK matrix is about compiling and the broker version is
about behaviour, and multiplying them would triple the container time to
re-prove what one run already shows.

If that job goes red, this page is wrong and the change that made it red is the
one to look at.

## Related

- [Topology](topology.md) — declaring queues, and how drift is detected
- [Streams](streams.md) — what a stream needs from the broker
- [Testing](testing.md) — the in-memory broker, and testing against a real one
