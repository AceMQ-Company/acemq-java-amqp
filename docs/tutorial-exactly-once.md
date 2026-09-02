# Tutorial 3 — Never processing twice

**25 minutes.** Continues from [tutorial 2](tutorial-surviving-failure.html).
Needs Docker and a database.

Tutorial 2 gave you retries. Retries create duplicates — that is not a bug in the
design, it is the design. This tutorial is about what that means when the handler
takes money.

## The thing to understand first

**There is no exactly-once delivery over a network.** Not here, not in Kafka, not
anywhere. The broker sends a message and waits for an acknowledgement. If the
acknowledgement does not arrive, the broker cannot tell these apart:

- the message never arrived;
- it arrived, was processed, and the acknowledgement was lost.

It must choose. Deliver again and risk processing twice, or do not and risk
losing it. Every broker worth using chooses **at least once**, because a
duplicate is a problem you can solve and a lost message is not.

So "exactly once" is not a delivery guarantee you switch on. It is **at-least-once
delivery plus an idempotent consumer**, and the second half is yours. This
tutorial builds it.

## Step 1 — See the duplicate

```java
AtomicInteger charged = new AtomicInteger();

mq.consume("payments.new", Payment.class,
        ConsumerOptions.prefetch(1).withRetry(RetryPolicy.fixed(3, Duration.ofSeconds(1))),
        message -> {
            charged.incrementAndGet();                     // the money moves
            System.out.println("charged, total " + charged.get());
            throw new IllegalStateException("the receipt email service is down");
        });
```

The charge succeeds. The email fails. The message retries, and **charges again**.

```
charged, total 1
charged, total 2
charged, total 3
```

Three charges for one payment, and the customer is right to be annoyed. The
failure had nothing to do with the charge.

## Step 2 — Remember what you have done

```java
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;

IdempotencyStore seen = InMemoryIdempotencyStore.forOneDay();

mq.consume("payments.new", Payment.class,
        ConsumerOptions.prefetch(1).withRetry(policy).idempotent(seen),
        message -> {
            charged.incrementAndGet();
            throw new IllegalStateException("the receipt email service is down");
        });
```

```
charged, total 1
```

The store is keyed on the **message id**, which AceMQ sets on publish and which
survives the retry ladder. Every attempt after the first sees the id already
recorded and skips the handler.

### Claim before, confirm after

The store has three operations, and the order they are used in is the whole
design:

| | |
|---|---|
| `claim(id)` | Before the handler. `false` means somebody already has it |
| `confirm(id)` | After the handler returns. Now it is permanently done |
| `release(id)` | After the handler throws. Someone else may try |

**The claim happens before the work, not after.** Marking afterwards leaves a
window — process dies between the charge and the mark — where the charge happened
and nothing recorded it, and the retry charges again. The whole point is to close
that window, so the mark must come first.

The cost is the opposite failure: the process dies *after* claiming and *before*
charging, and the claim blocks a retry that should have happened. That is why a
claim has a **lease** rather than being permanent — if it is not confirmed within
the claim timeout, it expires and the message can be tried again.

Which trade you want is real, and this one is deliberate: a payment not taken is
a support ticket, a payment taken twice is a chargeback.

## Step 3 — Make it survive a restart

`InMemoryIdempotencyStore` is a `HashMap`. Restart the process and it has
forgotten everything, so every in-flight message is a duplicate waiting to
happen. Worse, two replicas of the same service do not share one, so each
processes the message once — "once each" is not once.

```java
import org.acemq.amqp.patterns.JdbcIdempotencyStore;

JdbcIdempotencyStore seen = new JdbcIdempotencyStore(dataSource);
seen.createSchemaIfAbsent();          // development only; see below
```

Now the record is a row, `claim` is an insert, and the uniqueness is the
database's problem — which is a problem databases are extremely good at. Two
replicas racing to claim the same id: one insert wins, the other gets a
constraint violation and is told `false`.

`createSchemaIfAbsent()` is for development. In production the table belongs in
whatever migration tool already owns your schema — a library that creates tables
at start-up is a library deciding when your database changes.

Old rows are not kept forever:

```java
seen.purgeExpired();     // run this on a schedule
```

Retention is how far back a duplicate can arrive. A day is usually plenty; it
should comfortably exceed your longest retry ladder.

## Step 4 — The other duplicate, and the harder one

Idempotency fixes duplicates on the *consuming* side. There is a matching problem
on the publishing side, and it is worse because it produces **missing** messages
rather than extra ones:

```java
// Do not do this.
@Transactional
public void placeOrder(Order order) {
    orders.save(order);                                          // database
    mq.publisher("orders", "order.placed").send(order);          // broker
}
```

Two systems, one `@Transactional` that only covers one of them. Three outcomes:

1. Both succeed. Fine.
2. The save fails. Nothing published. Fine.
3. **The save succeeds and the publish fails** — or succeeds and the transaction
   then rolls back. The order exists and nobody was told.

Number three is the one that pages you at 3am, because the order is genuinely in
the database and the warehouse genuinely never heard about it, and no log
anywhere says so.

## Step 5 — The outbox

Write the message to the same database, in the same transaction, as the thing it
describes. Then one commit decides both.

```java
import org.acemq.amqp.patterns.JdbcOutboxStore;
import org.acemq.amqp.patterns.OutboxRelay;

JdbcOutboxStore outbox = new JdbcOutboxStore(
        () -> currentTransactionConnection(),   // the transaction's own connection
        relayDataSource);                       // a separate pool for the relay
outbox.createSchemaIfAbsent();
```

In your transaction:

```java
connection.setAutoCommit(false);
try {
    orders.save(connection, order);
    outbox.add(OutboxRecord.of("orders", "order.placed", envelope, json));
    connection.commit();     // ← one decision, both rows
} catch (Exception e) {
    connection.rollback();
}
```

**The two writes must use the same connection.** That is why the store takes a
`ConnectionSupplier` rather than a `DataSource` for the write side: a record
written on a different connection is a record in a different transaction, which
is the bug this pattern exists to prevent, reproduced faithfully.

Then a relay publishes what was committed:

```java
try (OutboxRelay relay = new OutboxRelay(mq, outbox)) {
    relay.start();
    // ...
}
```

It polls, claims a batch with a lease, publishes, and marks them published. A
relay that dies mid-batch loses its lease and another picks the batch up.

### What the outbox actually gives you

**Not** exactly-once publishing. The relay can publish a message and die before
recording that it did, and the next relay publishes it again. What it gives you is
**at-least-once publishing atomic with the database write** — the message is never
lost and never sent for work that rolled back.

The duplicate that remains is handled by step 2, on the consumer. That is the
whole architecture: the outbox stops messages going missing, idempotency stops
them counting twice, and together they are what people mean by "exactly once".

## Step 6 — Order of operations

For a handler that takes money, in order:

1. `claim` the message id. Stop if it is already claimed.
2. Do the work, in a database transaction.
3. Write any resulting messages to the outbox **in that same transaction**.
4. Commit.
5. `confirm` the message id.
6. Return, and let the acknowledgement happen.

Every step exists to close a window opened by the one before it.

## What to watch in production

| | |
|---|---|
| Outbox table depth | Rising means the relay is behind or dead. Messages exist and nobody has been told |
| Age of the oldest unpublished row | The real number. Depth is fine if it drains |
| Idempotency table size | Should plateau. Growing forever means nothing is purging |
| Claims that returned `false` | Your actual duplicate rate. Zero forever means either nothing retries or the store is not wired in |

## Next

**[Tutorial 4 — Seeing what happens](tutorial-observability.html).** All of the
numbers above, and where they come from.
