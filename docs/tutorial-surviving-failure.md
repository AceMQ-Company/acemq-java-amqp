# Tutorial 2 — Surviving failure

**20 minutes.** Continues from [tutorial 1](tutorial-first-message.html).

Your handler throws. This tutorial is about what should happen next, and about
the answer most services reach for first — which is wrong in a way that takes a
production incident to notice.

## Step 1 — Watch it fail

```java
mq.consume("orders.new", Order.class, message -> {
    System.out.println("attempt " + message.attempt());
    throw new IllegalStateException("the inventory service is down");
});
```

One line, one attempt, and the message is gone. A handler that throws rejects the
message, and a rejected message with nowhere to go is discarded.

That is the default because the alternative default — requeue — is worse. A
message that fails, requeues, is redelivered instantly, fails again, and requeues
is a **poison message loop**: a single bad message saturating a consumer at
thousands of attempts a second, with nothing else getting through.

## Step 2 — The answer that looks right

Almost every service reaches for this first:

```java
mq.consume("orders.new", Order.class, message -> {
    for (int attempt = 1; attempt <= 3; attempt++) {
        try {
            process(message.payload());
            return;
        } catch (Exception e) {
            Thread.sleep(1000);          // ← the problem
        }
    }
});
```

**Do not do this.** The sleep is inside the consumer, which means:

- The channel is held for the whole three seconds. With a prefetch of 10, ten
  messages are now sitting in this consumer's memory, unacknowledged, going
  nowhere.
- Everything behind the failure waits for it. One unavailable downstream service
  turns into a stalled queue.
- The broker cannot tell a slow consumer from a stuck one. Your consumer looks
  healthy while doing nothing.
- If the process dies mid-sleep, the retry count dies with it.

The last one is the real problem. **The retry state is in a local variable**, so
it exists only as long as the process does.

## Step 3 — Put the waiting in the broker

```java
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.ConsumerOptions;

RetryPolicy policy = RetryPolicy.exponential(
        4,                          // attempts, including the first
        Duration.ofSeconds(1),      // first delay
        Duration.ofMinutes(1));     // ceiling

mq.consume("orders.new", Order.class,
        ConsumerOptions.prefetch(10).withRetry(policy),
        message -> {
            System.out.println("attempt " + message.attempt());
            throw new IllegalStateException("the inventory service is down");
        });
```

Nothing sleeps. When the handler throws, the message is published to a **delay
queue** whose only job is to hold it for one second and then route it back. The
consumer thread is free immediately.

AceMQ declares those queues for you. After the first failure you will have:

```
orders.new                 your queue
orders.new.retry.1s        a rung: TTL 1s, dead-letters back to orders.new
orders.new.retry.5s        a rung
orders.new.retry.25s       a rung
orders.new.dlq             attempts exhausted
orders.new.parked          could not even be decoded
```

**One rung per distinct delay, not one queue per message.** The delay is a
property of the queue, so a thousand waiting messages cost one queue.

The attempt count travels in the message headers, so it survives a restart, a
redeploy, and a different consumer picking it up. That is the thing the local
variable could not do.

### Why the ceiling matters

`exponential(4, 1s, 1m)` gives 1s, 5s, 25s — and the ceiling stops it before
delays grow past anything useful. Without one, attempt 10 of an exponential
backoff is several hours away, which is the same as never but harder to diagnose.

Add `.withJitter(0.2)` when many consumers fail together: without it they all
retry at the same instant and hit the recovering service simultaneously.

## Step 4 — Say which failures are worth retrying

Retrying is only correct for failures that might not happen next time. A
malformed message will be malformed on every attempt, and retrying it four times
just delays the inevitable by half a minute.

```java
mq.consume("orders.new", Order.class, options, message -> {
    Order order = message.payload();

    if (order.getAmount() <= 0) {
        // Never going to succeed. Do not spend four attempts finding out.
        throw new AceFatalException("amount must be positive, was " + order.getAmount());
    }

    inventory.reserve(order);   // may throw; that one is worth retrying
});
```

| | |
|---|---|
| `AceFatalException` | Skips the ladder entirely. Straight to the dead-letter queue |
| `AceRetryableException` | Explicitly retryable |
| Anything else | Retried, on the assumption that an unclassified failure might be transient |

The default is to retry because the cost of retrying a permanent failure is a
delay, and the cost of not retrying a transient one is a lost message.

## Step 5 — Read the dead letters

After four attempts the message lands in `orders.new.dlq`, and it carries why:

```java
mq.consume("orders.new.dlq", Order.class, message -> {
    System.out.println("gave up on " + message.payload().getId());
    System.out.println("  attempts: " + message.attempt());
    System.out.println("  reason:   " + message.envelope().error().orElse("unknown"));
});
```

```
gave up on o-1
  attempts: 4
  reason:   exhausted 4 attempts: IllegalStateException: the inventory service is down
```

A dead-letter queue whose messages do not say why they are there is a queue
nobody can act on. The reason and the attempt count are attached when the message
is dead-lettered, not reconstructed later from logs.

## Step 6 — Replay them

The downstream service is fixed. The messages are still in the dead-letter queue.

```java
int moved = mq.replay("orders.new").replayAll();
System.out.println("replayed " + moved);
```

`replay(queue)` moves messages from that queue's dead-letter queue back to it.
`replay(queue).parked()` does the same for the parked queue.

Replay in batches when you are not sure, and filter when only some of them should
go back:

```java
mq.replay("orders.new").replay(100);   // the first hundred only
```

Replayed messages are marked — `envelope().replayedFrom()` and `replayCount()` —
so a message going round for the third time is visible as such rather than
looking like a fresh one.

## Step 7 — All together

```java
import java.time.Duration;
import java.util.Collections;

import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.transport.QueueType;

public class SurvivingFailure {

    public static void main(String[] args) throws Exception {
        RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(1)).withJitter(0);

        try (AceMq mq = AceMq.connect("memory://tutorial-2")) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("orders.new", "orders", "order.*");

            mq.consume("orders.new", Order.class,
                    ConsumerOptions.prefetch(1).withRetry(policy),
                    message -> {
                        System.out.println("attempt " + message.attempt()
                                + " for " + message.payload().getId());
                        throw new IllegalStateException("the inventory service is down");
                    });

            mq.publisher("orders", "order.placed", Order.class)
                    .send(new Order("o-1", 42.00));

            Thread.sleep(8_000);   // let the ladder run

            mq.consume("orders.new.dlq", Order.class, message ->
                    System.out.println("dead: " + message.payload().getId()
                            + " after " + message.attempt() + " attempts"
                            + " — " + message.envelope().error().orElse("unknown")));

            Thread.sleep(1_000);
        }
    }
}
```

```
attempt 1 for o-1
attempt 2 for o-1
attempt 3 for o-1
dead: o-1 after 3 attempts -- exhausted 3 attempts: IllegalStateException: the inventory service is down
```

Note the gaps between the attempts, and note that nothing slept in a handler to
produce them.

## What to watch in production

| | |
|---|---|
| Depth of `*.dlq` | Rising means something is failing permanently. This is the alert |
| Depth of `*.retry.*` | Rising means something is failing transiently, right now |
| Depth of `*.parked` | Anything above zero is a message nothing can decode — usually a publisher deploying a format change ahead of its consumers |

## Next

**[Tutorial 3 — Never processing twice](tutorial-exactly-once.html).** Retries
create duplicates by construction. What that means for taking money.
