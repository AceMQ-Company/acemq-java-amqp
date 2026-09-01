# AceMQ for Java

A messaging library for RabbitMQ built on one idea: **a message should never
disappear quietly.**

Every default here is the safe one. Publishes wait for the broker to confirm
them. A message nothing is bound to receive is an error, not a shrug. A handler
that fails gets retried on a schedule the broker enforces, and if it never
succeeds the message ends up somewhere you can find it and put it back. None of
that is opt-in.

```java
try (AceMq mq = AceMq.connect("amqp://localhost")) {
    mq.declareExchange("orders", "topic");
    mq.declareQueue("orders.new");
    mq.bind("orders.new", "orders", "order.*");

    mq.publisher("orders", "order.placed", Order.class)
      .send(new Order("o-1", 42.00));

    mq.consume("orders.new", Order.class, message -> process(message.payload()));
}
```

That is the whole thing: connect, declare, publish, consume. JSON serialization,
publisher confirms, acknowledgement after the handler returns, and connection
recovery are already on.

## Start here

| | |
|---|---|
| [Getting started](getting-started.html) | Install it and send your first message |
| [Publishing](publishing.html) | Confirms, unroutable messages, delivery options |
| [Consuming](consuming.html) | Prefetch, acknowledgement, concurrency |
| [Reliability](reliability.html) | Retries, dead letters, replay, idempotency |
| [Serialization](serialization.html) | JSON, XML, YAML, Avro, Protobuf |
| [Streams](streams.html) | Append-only logs, offsets, replay |
| [Security](security.html) | TLS, credentials, and what is not built yet |
| [Testing](testing.html) | An in-memory broker, no Docker needed |
| [API reference](apidocs/index.html) | Javadoc for every module |
| [Licence and warranty](licence.html) | Apache-2.0, what it disclaims, and where support comes from |

## What it is not

It is not a framework. There is no container, no annotations, no lifecycle to
learn — the entry point is a class you construct and close.

It is not a broker abstraction that pretends every broker is the same. When a
transport cannot do something, it says so and fails, rather than silently doing
something close enough. A stream is not a queue with different settings, and
this library will not let you treat it as one.

It is not finished. See [what is missing](#status) below before you depend on it.

## Status

Pre-1.0, and honest about it. What works today is tested against real RabbitMQ in
CI on every commit: publishing with confirms, consuming, the retry ladder,
dead-letter and parking queues, replay, streams, pipelines, ordered consumption,
the transactional outbox, idempotency (in-process and shared), interceptors, and
five serialization formats.

Still to come: batch and asynchronous publishing, a Spring Boot starter, and a
RabbitMQ 3.13 compatibility matrix. Coordinates and API shape may still move
before 1.0, which is why artifacts are not yet on Maven Central.
