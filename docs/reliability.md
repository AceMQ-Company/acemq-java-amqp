# Reliability

What happens when things fail, which is the reason this library exists.

## Retries, without a sleeping thread

```java
mq.consume("orders.new", Order.class,
        ConsumerOptions.prefetch(20).withRetry(
                RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofMinutes(5))),
        message -> payments.charge(message.payload()));
```

The delay happens **in the broker**, not in your process. The policy generates a
ladder of queues with time-to-live and a dead-letter target pointing home:

```
orders.new.retry.1s     ttl 1s   -> orders.new
orders.new.retry.5s     ttl 5s   -> orders.new
orders.new.retry.25s    ttl 25s  -> orders.new
orders.new.dlq                   (attempts exhausted)
orders.new.parked                (could not be decoded)
```

A message needing a five-second wait is published into the five-second rung,
expires, and is routed back. No consumer is involved and no thread waits — which
is why a retry storm does not exhaust your thread pool, and why retries survive a
restart of your service.

Attempt count travels on the message (`envelope.attempt()`), never in a counter
on your side.

## Dead letters and the parking lot

Two destinations, because the two failures need different fixes:

- **`<queue>.dlq`** — the handler ran and kept failing, or threw
  `AceFatalException`. A retry might genuinely work later.
- **`<queue>.parked`** — the payload could not be *decoded*. It will fail
  identically every time until the code changes, so it never enters the ladder.
  The original bytes are kept exactly as received.

## Replay

Capturing a failed message is half a feature. A dead-letter queue nobody can
drain is a slower way of losing data.

```java
Replay replay = mq.replay("orders.new");

replay.pending();              // 412 waiting — look before touching
replay.replay(50);             // move a bounded batch back
replay.replayAll();

replay.parked().replayAll();   // the undecodable ones, after deploying the fix
```

Messages go back to the **queue** they failed in, not through the exchange that
first routed them — republishing through the exchange would deliver to every
bound queue and hand duplicate work to consumers that never failed.

The body is returned byte for byte. The attempt counter resets so the message
gets the whole ladder again instead of arriving exhausted, and provenance is
recorded on the envelope:

```java
message.envelope().replayedFrom();   // "orders.new.dlq"
message.envelope().replayedAt();
message.envelope().replayCount();    // 5 means this has been round five times
```

`replayCount` is worth reading in a handler. A message on its fifth trip through
the dead-letter queue is telling you something a reset attempt counter hides.

Replay is at-least-once: each message is published to the source queue and only
then acknowledged in the dead-letter queue, so a crash between the two replays it
again. Acknowledging first would lose it, which is the wrong way round for a tool
whose whole job is not losing things.

## Idempotency

Every broker worth using delivers at least once, so duplicates are normal
traffic, not an error. Handling one twice is your problem to prevent:

```java
// One process. Fast, and forgets everything on restart.
ConsumerOptions.prefetch(20).idempotent(InMemoryIdempotencyStore.forOneDay());

// Several processes behind one queue.
JdbcIdempotencyStore store = new JdbcIdempotencyStore(dataSource);
store.createSchemaIfAbsent();          // development only; production uses migrations
ConsumerOptions.prefetch(20).idempotent(store);
```

The in-process store is useless the moment there are three instances behind one
queue: the redelivery lands on a different machine, finds an empty map, and
charges the card again.

**A shared store has a failure the in-process one cannot have.** It outlives the
process, so a consumer that dies mid-handler leaves its claim behind — and
without an expiry, every future redelivery of that message is discarded as a
duplicate. One crash becomes silent message loss. So a claim is a *lease*
(`claimTimeout`, five minutes by default) another consumer may take over. Set it
comfortably above your slowest handler: too short and two consumers work the same
message at once; too long and a crash stalls that message.

Schedule `purgeExpired()`. Nothing on the message path deletes rows, because a
store that tidies up on the hot path makes every message pay for it.

This deduplicates the *delivery*, not the work. If your handler writes to a
different database from this table, a crash between that write committing and the
confirm landing leaves the work done and unrecorded.

## The transactional outbox

The dual-write problem: you save an order and publish an event, and the process
dies between them. Solved by writing the message in the same transaction as the
business data:

```java
@Transactional
public void placeOrder(Order order) {
    orders.save(order);
    outbox.enqueue("orders", "order.placed", new OrderPlaced(order.id()));
}
```

A relay publishes afterwards and marks each record done. The message becomes
durable exactly when your transaction commits — no distributed transaction, no
XA.

## Interceptors

For what belongs on every message rather than at every call site:

```java
mq.intercept(new ConsumeInterceptor() {
    public void beforeHandle(ConsumeContext context) {
        MDC.put("correlationId", context.envelope().correlationId());
    }
    public void afterHandle(ConsumeContext context, Ack ack) {
        MDC.remove("correlationId");
    }
});
```

`afterHandle` runs whether the handler succeeded or failed, and in reverse order,
so nested scopes close inside out. Throwing from `beforeHandle` fails the
delivery — it is retried and eventually dead-lettered, which is the honest
outcome for a refused message.
