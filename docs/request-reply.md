# Request and reply

```java
try (Responder responder = mq.respond("pricing", Quote.class, quote -> price(quote));
        Requester requester = mq.requester()) {

    Price price = requester.request("", "pricing", quote, Price.class, Duration.ofSeconds(5));
}
```

## Read this before using it

Request/reply over a broker is **synchronous calling wearing asynchronous
clothes**, and it inherits the worst of both. The caller blocks like an HTTP
client, and the failure modes are a message broker's.

If the two services can speak HTTP or gRPC, they should. Those tools have
timeouts, retries, load balancing, circuit breakers, health checks and tracing
that a messaging library will not match, and everyone already knows how to
operate them.

What this is genuinely for:

- the callee is reachable **only** on the broker — no HTTP endpoint, behind a
  firewall, in a network you do not control;
- one worker among many, where the broker is already doing the load balancing;
- a system that is already message-driven and where adding an HTTP hop would mean
  a second failure domain.

Those cases are real. Doing them by hand means reply queues, correlation ids, and
a timeout somebody always forgets.

## How it works

The request carries AMQP's own **`reply-to`** property naming a queue, and a
**correlation id**. The responder publishes the answer to that queue with the
same id, and the requester matches it to the caller waiting for it.

`reply-to` is a real AMQP property rather than an `x-acemq-*` header, deliberately:
a service written against this library can answer a caller that was not, and the
other way round.

One reply queue serves every request from a `Requester`. Build one per connection
— one per call creates and destroys a queue for every question you ask.

## Timeouts

```java
requester.request("", "pricing", quote, Price.class, Duration.ofSeconds(5));
```

A timeout throws `RequestTimedOutException`, and its message says the thing that
matters:

> The request may still be queued, being handled, or already done with the reply
> lost on the way back.

**A timeout is not a failure to handle the request.** Retrying is a decision
about whether the responder is idempotent, not a reflex. Where the work is not
idempotent — taking money, sending an email — a timeout is a question for a human
or for a [shared idempotency store](reliability.html), not for a retry loop.

`requestAsync` hands back a `CompletableFuture` with no timeout attached, because
the timeout belongs to the caller. `orTimeout` is the usual way to add one.

## The numbers worth graphing

| | |
|---|---|
| `requester.timedOut()` | Callers that gave up |
| `requester.unmatched()` | Replies that arrived with nobody waiting — almost always the timeout being too short |
| `responder.answered()` | Requests answered |
| `responder.unanswerable()` | Requests that arrived with no `reply-to`. Anything above zero means a caller is using `publish` where it means `request` |

`unmatched()` rising while `timedOut()` rises is the signature of a responder
that is slower than callers expect. Nothing is broken; the timeout is wrong.

## Concurrency

```java
mq.respond("pricing", Quote.class, ConsumerOptions.prefetch(10).concurrency(4), this::price)
```

A responder handles one request at a time by default, and **every caller behind a
slow one is blocked**. This is the setting to reach for first when request/reply
feels slow.

## Cleaning up

The reply queue is deleted when the requester closes, and carries `x-expires` so
the broker removes it if the process dies first. Without that, a service that
restarts often leaves thousands of orphaned queues — a real way to run a broker
out of memory with nothing obviously wrong.

Closing a `Responder` drains it: a request being answered right now has a caller
blocked on the other side, and dropping it turns their call into a timeout.

## Related

- [Publishing](publishing.html)
- [Consuming](consuming.html)
- [Reliability](reliability.html) — idempotency, which decides whether a timeout can be retried
