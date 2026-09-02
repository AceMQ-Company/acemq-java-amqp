# Testing

There is an in-memory transport. It needs no Docker, starts instantly, and
implements routing, prefetch and settlement — which is what most tests actually
exercise.

```java
try (AceMq mq = AceMq.connect("memory://orders")) {
    mq.declareExchange("orders", "topic");
    mq.declareQueue("orders.new", QueueType.CLASSIC, Map.of());
    mq.bind("orders.new", "orders", "order.*");

    mq.publisher("orders", "order.placed", Order.class).send(order);
}
```

Add it as a test dependency:

```xml
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-test</artifactId>
  <version>0.2.9</version>
  <scope>test</scope>
</dependency>
```

Two connections to the same name share a broker; a different name is a different
broker. Prefer a unique name per test over resetting shared state:

```java
AceMq.connect("memory://orders-" + UUID.randomUUID());
```

## It is a fake, not a simulator

This matters, and the library is deliberate about it. The in-memory broker
implements routing, prefetch, settlement, per-queue and per-message expiry, and
dead-lettering. It does **not** implement replication, persistence, delayed
delivery, or streams — and it does not *claim* those capabilities, so code
depending on them fails here for the same reason it would fail against a broker
that lacks them.

Concretely: **streams need a real broker.** `mq.declareStream(...)` on
`memory://` refuses rather than quietly creating a classic queue.

Where the fake and RabbitMQ genuinely differ, the difference is documented rather
than hidden. Blocked connections are the clearest example — see below.

## Testing failure

Simulating a broker under a memory alarm, which is otherwise nearly impossible
to reproduce:

```java
InMemoryTransport.block("orders", "low on memory");

assertThatThrownBy(() -> publisher.send(order))
        .isInstanceOf(ConnectionBlockedException.class);

InMemoryTransport.unblock("orders");
```

One deliberate difference: here `isBlocked()` becomes true immediately, whereas
RabbitMQ only tells a connection it is blocked when that connection next
publishes. Against a real broker the first message into an alarm is already on
the wire, so it *may* have arrived; here nothing is ever written, and
`mayHaveBeenPublished()` is always `false`. A test that needs the uncertain case
needs a real broker.

## Testing against a real broker

Use Testcontainers for the cases the fake cannot cover — streams, replication,
persistence, and anything where you want to be sure:

```java
@Container
static final RabbitMQContainer BROKER =
        new RabbitMQContainer(DockerImageName.parse("rabbitmq:4-management"));

AceMq mq = AceMq.connect(BROKER.getAmqpUrl());
```

The library's own suite does exactly this, and runs both: the fake for speed,
real RabbitMQ for truth. When the two disagree, the fake is wrong.
