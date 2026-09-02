# Tutorial 1 — Your first message

**10 minutes. No broker, no Docker, no configuration.**

By the end you will have published a message and consumed it, and you will know
what each of the four lines does.

## Step 1 — A project

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
    <version>0.2.10</version>
  </dependency>
  <dependency>
    <groupId>org.acemq</groupId>
    <artifactId>acemq-amqp-test</artifactId>
    <version>0.2.10</version>
  </dependency>
</dependencies>
```

`acemq-amqp-test` is what lets this tutorial skip Docker. It provides an
in-process broker behind a `memory://` URL that implements the same interface the
RabbitMQ one does, so everything you write here works unchanged against a real
broker later.

## Step 2 — Something to send

```java
public class Order {
    private String id;
    private double amount;

    public Order() {}                       // Jackson needs this

    public Order(String id, double amount) {
        this.id = id;
        this.amount = amount;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
}
```

A plain class with getters and a no-argument constructor. Nothing about it knows
it will be sent anywhere — no annotations, no base class, no interface. That is
deliberate: a domain type that has to be told about your message broker is a
domain type your broker now owns.

## Step 3 — Connect and declare

```java
import org.acemq.amqp.core.AceMq;

try (AceMq mq = AceMq.connect("memory://tutorial")) {
    mq.declareExchange("orders", "topic");
    mq.declareQueue("orders.new");
    mq.bind("orders.new", "orders", "order.*");
}
```

Three concepts, and they are the whole of AMQP routing:

| | |
|---|---|
| **Exchange** | Where you publish. It holds nothing; it decides where things go |
| **Queue** | Where messages wait. This is the thing that holds messages |
| **Binding** | The rule joining the two — "send anything matching `order.*` to `orders.new`" |

You never publish to a queue. You publish to an exchange and the bindings decide.
That indirection is the point: adding a second consumer later means adding a
binding, not changing the publisher.

`topic` means the routing key is matched as a pattern. `order.*` matches
`order.placed` and `order.cancelled`, not `order.line.added` — `*` is one word,
`#` is any number.

**`declareQueue` gives you a durable quorum queue**, because that is the right
default for anything holding real messages. The in-memory broker cannot do quorum
queues and will say so; against it, ask for a classic one:

```java
mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
```

Declaring is idempotent. Running this twice is fine; running it against a queue
that already exists **with different settings** is not, and AceMQ will tell you
which setting differs rather than failing at the broker. That is
[topology drift](topology.html), and it is tutorial 2's problem, not yours yet.

### The same thing, as data

Three calls is how every AMQP client does this, and it is the right way to *learn*
it — three concepts, one line each. It is not the way to run it. Once you know
what the three are, declare them as a value instead:

```java
Topology orders = Topology.define()
        .exchange("orders", "topic")
        .classicQueue("orders.new", Collections.emptyMap())
        .bind("orders.new", "orders", "order.*")
        .build();

mq.topology().apply(orders, ApplyMode.CREATE_ONLY);
```

Identical result. The difference is that the topology is now **a thing you can
hold**, and three useful things follow from that:

```java
mq.topology().apply(orders, ApplyMode.DRY_RUN);    // print the plan, change nothing
mq.topology().apply(orders, ApplyMode.VALIDATE);   // fail if it is not already there
System.out.println(mq.topology().plan(orders).render());
```

`DRY_RUN` is what a pull request should show. `VALIDATE` is for the environments
where an operator provisions the topology and an application finding it absent is
a deployment error rather than something to silently fix. And because the plan is
computed before anything is declared, a queue that exists with the *wrong*
settings is reported — with the argument named — instead of failing partway
through and taking the channel with it.

None of that is reachable from three imperative calls, because by the time the
second one runs the first has already happened.

**Use the builder in anything real.** The rest of this tutorial keeps the three
calls, because it is still teaching what they are.

## Step 4 — Consume

```java
mq.consume("orders.new", Order.class, message ->
        System.out.println("got " + message.payload().getId()));
```

Start the consumer **before** publishing. A message published to an exchange with
nothing bound behind it is discarded by the broker — that is AMQP, not AceMQ, and
it surprises everyone once.

The handler takes a `Message<Order>`, not an `Order`, because you will eventually
want the things around the payload: the attempt count, the headers, the message
id. `message.payload()` is the object.

**The message is acknowledged when your handler returns**, and rejected if it
throws. You do not call `ack()`. A handler that returned normally and then lost
the message to a crash before someone remembered to acknowledge is the single
most common way to lose a message, so the acknowledgement is tied to the thing
that actually indicates success.

## Step 5 — Publish

```java
mq.publisher("orders", "order.placed", Order.class)
        .send(new Order("o-1", 42.00));
```

`publisher(exchange, routingKey, type)` — where it goes, and what it carries.

`send` **blocks until the broker confirms it has the message.** Not until it is
written to a socket: until the broker says it took responsibility. Publisher
confirms are opt-in in AMQP and off by default in most clients, which is why "we
published it" and "it exists" are so often different facts. Here the safe thing
is the default one.

## Step 6 — All together

```java
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.QueueType;

public class FirstMessage {

    public static void main(String[] args) throws Exception {
        CountDownLatch received = new CountDownLatch(1);

        try (AceMq mq = AceMq.connect("memory://tutorial")) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            mq.consume("orders.new", Order.class, message -> {
                System.out.printf("got %s for %.2f%n",
                        message.payload().getId(), message.payload().getAmount());
                received.countDown();
            });

            mq.publisher("orders", "order.placed", Order.class)
                    .send(new Order("o-1", 42.00));

            // Only because main() would otherwise exit before the handler runs.
            // A real service does not do this; it stays up.
            received.await(5, TimeUnit.SECONDS);
        }
    }
}
```

```
got o-1 for 42.00
```

## What you did not have to do

No serializer configuration — JSON is the default and the content type travels
with the message, so a consumer reads whatever arrived. No acknowledgement
bookkeeping. No confirm handling. No prefetch tuning: it is bounded rather than
unlimited, which is what stops one consumer from pulling a whole queue into
memory.

Those are the defaults, and each one is the safe choice rather than the
convenient one. You can change every one of them by name.

## Against a real broker

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Change one line:

```java
try (AceMq mq = AceMq.connect("amqp://localhost")) {
```

and drop the `QueueType.CLASSIC` argument so you get the quorum queue you should
have in production. Nothing else changes. The management UI at
<http://localhost:15672> (guest/guest) will show the exchange, the queue and the
binding you declared.

## Next

**[Tutorial 2 — Surviving failure](tutorial-surviving-failure.html).** Your
handler throws. What should happen, what actually happens, and why sleeping in a
consumer is the wrong answer.
