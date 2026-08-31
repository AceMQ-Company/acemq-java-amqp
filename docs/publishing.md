# Publishing

```java
Publisher<Order> orders = mq.publisher("orders", "order.placed", Order.class);
PublishResult result = orders.send(new Order("o-1", 42.00));
```

A publisher writes to one destination in one format, and is safe to share
between threads. Create it once and keep it; creating one per message works but
is wasteful.

## What a confirm promises

`send` blocks until the broker confirms the message, and throws
`PublishFailedException` if it does not. That is the entire point: a publish that
returns normally means RabbitMQ has taken responsibility for the message.

It does **not** mean the message was consumed, or even written to disk — see
`transientDelivery()` below. It means the broker accepted it.

Two failures are reported that most clients let pass silently:

```java
try {
    orders.send(order);
} catch (PublishFailedException e) {
    // Either the broker rejected the message, or nothing was bound to receive
    // it. The second one is nearly always a typo in a routing key.
}
```

## Naming the payload type

```java
mq.publisher("orders", "order.placed", Order.class);   // preferred
mq.publisher("orders", "order.placed");                // infers from context
```

Prefer the three-argument form. The two-argument one infers its type from
whatever you assign it to, so it will happily hand you a `Publisher<Object>` and
let you send something no consumer can read.

## Delivery options

The defaults — persistent, and unroutable-is-an-error — are right for most
messages and wrong for some. `PublishOptions` is how you say so out loud:

```java
// Telemetry: high volume, and losing some in a restart is fine.
mq.publisher("metrics", "cpu.sample", Sample.class,
        PublishOptions.transientDelivery()
                      .expiringAfter(Duration.ofMinutes(5)));

// A fan-out nobody is required to be listening to yet.
mq.publisher("audit", "user.login", LoginEvent.class,
        PublishOptions.defaults().allowUnroutable());
```

| Option | Default | What turning it off costs |
|---|---|---|
| `persistent` | on | a broker restart may lose the message, even from a durable queue |
| `mandatory` | on | an unroutable message is dropped silently instead of raising |
| `expiringAfter` | none | opt-in: the broker discards the message when it runs out of time |

Expiry is not a promise of promptness. RabbitMQ removes an expired message when
it reaches the head of the queue, so one stuck behind a backlog is discarded
rather than delivered late — a consumer must not assume everything it receives is
inside its window.

Options travel with the publisher, including through `asXml()` and friends:
asking for a different format is not a request to start writing to disk again.

## When the broker stops accepting messages

RabbitMQ blocks publishing connections when it runs low on memory or disk. It
does not close them and does not return an error — the socket simply stops
draining, and an unguarded publisher waits forever with nothing in the logs.

This library waits, bounded, and then tells you:

```java
AceMq mq = AceMq.connect(ConnectionConfig.url("amqp://broker")
        .blockedTimeout(Duration.ofSeconds(30))     // the default
        .build());

try {
    orders.send(order);
} catch (ConnectionBlockedException e) {
    e.reason();                  // "low on memory" or "low on disk"
    e.mayHaveBeenPublished();    // false → certainly not sent, safe to resend
}
```

Waiting rather than failing immediately because a memory alarm is usually brief,
and turning every one into an application error replaces a pause with an outage.

`mq.isBlocked()` belongs in a **readiness** probe, never a liveness one. A
blocked broker is under pressure, not broken, and a service that reports itself
dead every time an alarm fires gets restarted by its orchestrator while the
broker is recovering on its own. Consuming deliberately keeps working while
publishing is blocked — draining queues is how the broker gets out of the alarm.

## Cross-cutting concerns

Anything that belongs on *every* message goes in an interceptor rather than in
every call site:

```java
mq.intercept((PublishInterceptor) context ->
        context.withEnvelope(context.envelope().toBuilder()
                .header("tenant", TenantContext.current())
                .build()));
```

Interceptors run before encoding, so they see your object rather than bytes.
Throwing from one refuses the publish — which is what makes them usable for
policy. See [Reliability](reliability.html#interceptors).
