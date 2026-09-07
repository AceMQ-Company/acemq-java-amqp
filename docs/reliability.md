# Reliability

What happens when things fail, which is the reason this library exists.

## Retries, and where the waiting happens

```java
mq.consume("orders.new", Order.class,
        ConsumerOptions.prefetch(20).withRetry(
                RetryPolicy.exponential(5, Duration.ofSeconds(10), Duration.ofMinutes(5))),
        message -> payments.charge(message.payload()));
```

`exponential` doubles: this one waits 10s, 20s, 40s and 80s. Twenty percent
jitter is applied to each, in both directions, so a downstream outage that fails
a thousand messages at once does not retry all thousand on the same tick.
`RetryPolicy.fixed` applies none, because "every thirty seconds" is an exact
statement.

**A short wait is spent in the consumer, a long one in the broker.** The line
between them is thirty seconds by default. Below it, the consumer holds the
delivery and one prefetch slot for the duration; above it, the message is
published into a rung queue whose `x-message-ttl` is the wait and whose
dead-letter target is the queue it came from, and the broker returns it when the
time is up.

So the policy above produces a ladder with two rungs, not four:

```
orders.new.retry.40s    ttl 40s   -> orders.new
orders.new.retry.80s    ttl 80s   -> orders.new
orders.new.dlq                    (attempts exhausted, or too old)
orders.new.parked                 (could not be decoded)
```

The 10s and 20s waits get no queue at all. That is the point of having a
threshold rather than one rule: a schedule that runs in a few seconds costs the
broker nothing, and the seconds a restart loses are only seconds. Above the
threshold the arithmetic changes — a consumer sleeping on a five-minute backoff
that restarts at minute one does not resume at minute one, because the broker
redelivers the unacknowledged message immediately and a five-minute policy
delivers in none. That is a correctness bug rather than a throughput one, and it
is what the rung queues are for. Nothing consumes a rung; the time-to-live is the
only thing that ever takes a message out of one.

Move the line, or turn the broker half off entirely:

```java
policy.waitInBrokerFrom(Duration.ofMinutes(1));  // rungs only past a minute
policy.waitInBrokerFrom(Duration.ZERO);          // never; every wait is local
```

Zero is the setting for a service that is not allowed to declare queues on its
broker. It is the wrong setting for a policy whose delays are measured in
minutes.

Jitter is never applied to a wait the broker holds. A rung's time-to-live is
fixed when the queue is declared, so a jittered delay would name a queue that
does not exist — and the spread comes free up there anyway, because each
message's time-to-live starts when it arrives on the rung rather than when the
batch failed.

Attempt count travels on the message (`envelope.attempt()`), never in a counter
on your side. A retry is **republished** with that count advanced, whichever of
the two held the wait: a requeue would return the bytes the broker was given, so
the count would read what the publisher wrote however many times the message had
come round.

The threshold is thirty seconds in the Go, .NET, Python and Ruby libraries too,
so the same policy needs the same rungs whichever of them declares the topology.

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
replay.keepingAttempts().replayAll();   // put back exactly what was there
```

Messages go back to the **queue** they failed in, not through the exchange that
first routed them — republishing through the exchange would deliver to every
bound queue and hand duplicate work to consumers that never failed.

The body is returned byte for byte. The attempt counter resets so the message
gets the whole ladder again instead of arriving exhausted — a message
dead-lettered on the last attempt of a five-attempt policy would otherwise be
dead-lettered again before any handler saw it. `keepingAttempts()` turns that
off, for an audit or for a queue read by something that counts attempts itself.
Provenance is recorded on the envelope either way:

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
