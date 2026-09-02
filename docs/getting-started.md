# Getting started

## Install

Artifacts are published to the AceMQ Maven repository. Add it alongside your
other repositories — no credentials are needed.

```xml
<repositories>
  <repository>
    <id>acemq</id>
    <url>https://acemq-company.github.io/maven/</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>org.acemq</groupId>
    <artifactId>acemq-amqp-core</artifactId>
    <version>0.2.9</version>
  </dependency>
  <dependency>
    <groupId>org.acemq</groupId>
    <artifactId>acemq-transport-rabbitmq</artifactId>
    <version>0.2.9</version>
    <scope>runtime</scope>
  </dependency>
</dependencies>
```

Gradle:

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://acemq-company.github.io/maven/") }
}

dependencies {
    implementation("org.acemq:acemq-amqp-core:0.2.9")
    runtimeOnly("org.acemq:acemq-transport-rabbitmq:0.2.9")
}
```

Two artifacts, and the split is deliberate. `acemq-amqp-core` is the engine and
knows nothing about RabbitMQ; `acemq-transport-rabbitmq` is the transport,
discovered at runtime by the `amqp://` scheme in your URL. That is why it is a
runtime dependency: your code should not compile against it.

Java 17 or later.

## A broker to talk to

```bash
docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

The management UI is at <http://localhost:15672>, guest/guest. Worth having open
the first time — everything this library does shows up there.

## Send and receive

```java
import org.acemq.amqp.core.AceMq;

public record Order(String id, double total) { }

try (AceMq mq = AceMq.connect("amqp://localhost")) {
    mq.declareExchange("orders", "topic");
    mq.declareQueue("orders.new");
    mq.bind("orders.new", "orders", "order.*");

    mq.consume("orders.new", Order.class,
            message -> System.out.println("got " + message.payload()));

    mq.publisher("orders", "order.placed", Order.class)
      .send(new Order("o-1", 42.00));

    Thread.sleep(1000);   // the consumer runs on its own threads
}
```

That prints `got Order[id=o-1, total=42.0]`.

### Declaring topology as data instead

The three `declare` calls are the shortest thing that works, and they are what
every AMQP client offers. For anything you deploy, describe the topology as a
value and let AceMQ work out what applying it would do:

```java
Topology orders = Topology.define()
        .exchange("orders", "topic")
        .queue("orders.new")                       // durable quorum
        .bind("orders.new", "orders", "order.*")
        .build();

mq.topology().apply(orders, ApplyMode.DRY_RUN);    // print the plan, change nothing
mq.topology().apply(orders, ApplyMode.CREATE_ONLY);
```

Same result, but the topology is now something you can print in a pull request,
validate against an environment you do not provision, and — the part that matters
in production — **check before touching anything**, so a queue that already
exists with different settings is reported with the offending argument named
rather than failing partway through and closing the channel.

[Topology](topology.html) covers the modes and what drift looks like.

## What just happened

More than the code suggests, and all of it on purpose:

- **The publish waited for the broker to confirm it.** `send` does not return
  until RabbitMQ has accepted responsibility for the message. A publish that
  fails throws.
- **An unroutable message would have failed.** If nothing had been bound for
  `order.placed`, you would have got a `PublishFailedException` rather than
  silence. A routing-key typo is the easiest way to lose every message you send,
  so it is an error here.
- **The payload became JSON**, because nobody said otherwise.
- **The message was acknowledged after your handler returned**, not when it was
  delivered. If the handler throws, the message is not lost.
- **`close()` stopped the consumer** and let in-flight handlers finish.

## About that URL

`amqp://localhost` is plaintext, which is right for a broker running on your own
machine and wrong for anything else. In production the URL is `amqps://`, and the
certificate is verified by default:

```java
AceMq.connect("amqps://broker.internal:5671");
```

Credentials belong in a `CredentialsProvider` rather than in the URL — see
[Security](security.html), which also states plainly which parts of the security
story are not built yet.

## Where to go next

Publishing options and what a confirm really promises: [Publishing](publishing.html).
Prefetch, concurrency and acknowledgement: [Consuming](consuming.html).
What happens when a handler keeps failing: [Reliability](reliability.html).

And before you write a test against a real broker, read
[Testing](testing.html) — there is an in-memory transport that needs no Docker.
