# Serialization

Nobody should have to think about this for the common case, so the common case
needs no configuration:

```java
mq.publisher("orders", "order.placed", Order.class).send(order);   // JSON
mq.consume("orders.new", Order.class, m -> handle(m.payload()));   // decoded
```

## Write one format, read all

A publisher writes **one** format. A consumer reads **every** format on the
classpath. That asymmetry is deliberate and it is what makes changing format two
ordinary releases instead of a flag day:

1. Deploy consumers that can read the new format. They still read the old one.
2. Deploy publishers that write it.

The bytes on a queue are a contract with services that are not being redeployed
right now. A queue carrying two formats at once is a queue nobody can write a
consumer against — so the place to choose is once, where the destination is
named.

## Choosing a format

```java
mq.publisher("legacy", "order", Order.class).asXml();
mq.publisher("config", "changed", Config.class).asYaml();
mq.publisher("flags", "changed", Flags.class).as(Codecs.byName("toml"));
mq.publisher("files", "file.new", byte[].class).asBytes();
mq.publisher("logs", "line", String.class).asText();
mq.publisher("events", "order", Order.class).as(new AvroCodec(registry));
```

Each returns a **new** publisher; the original is untouched. A long-lived object
that quietly changes what it writes is worse than one that does not.

Add the module for anything beyond JSON:

| Format | Artifact |
|---|---|
| JSON (default) | `acemq-amqp-codec-json` |
| XML | `acemq-amqp-codec-xml` |
| YAML | `acemq-amqp-codec-yaml` |
| TOML | `acemq-amqp-codec-toml` |
| Avro | `acemq-amqp-codec-avro` |
| Protobuf | `acemq-amqp-codec-protobuf` |

Codecs are found by `ServiceLoader`, so adding the dependency is the whole
installation. Asking for a format whose module is absent fails immediately, by
name, rather than at the first message.

## YAML or TOML

Both are for the same audience: a message a person reads and edits as well as a
program. They are text, larger than JSON and slower to parse, so neither belongs
on a high-volume path — they earn their place where somebody actually looks at
the message.

Between them:

| | |
|---|---|
| **YAML** | Nested structures. Familiar from Kubernetes and CI files |
| **TOML** | Flat-ish configuration. One way to write a string, no significant indentation, and `country = NO` is an error rather than a boolean |

TOML's rigidity is the point. YAML has a dozen ways to write the same thing and a
famous habit of turning `NO` into `false`; if a person edits the message and a
machine acts on it, fewer ways to be wrong is worth more than expressiveness.

**A TOML document is a table.** The top level has to be an object — a list, a
string or a number has no TOML representation, and the codec refuses rather than
writing something back. That refusal exists because Jackson does not: given a
list it writes ` = ['a', 'b']`, a key-less assignment that its own parser then
rejects. Deeply nested payloads read poorly in TOML too; where the message is a
tree rather than a table, JSON is the honest answer.

## Avro and Protobuf

Neither can be built without a schema, so neither has a no-argument shortcut —
`asAvro()` would only be able to fail:

```java
Codec avro = new AvroCodec(schemaRegistry, OrderPlaced.getClassSchema());
mq.publisher("events", "order.placed", OrderPlaced.class).as(avro);
```

Avro uses the Confluent wire framing — a zero byte, a four-byte big-endian schema
id, then the body — so consumers written against other Confluent-compatible
tooling can read it.

## The schema registry

Avro's bytes and Protobuf's carry no account of what they are. A reader has to
already hold the schema the writer used, and what travels in the message is a
small integer standing for it. Something has to remember which integer means
which schema.

```java
JdbcSchemaRegistry registry = new JdbcSchemaRegistry(dataSource);
registry.createSchemaIfAbsent();
```

**Identifiers have to be stable forever.** A message published today may be read
next year by a consumer looking up the schema it was written with. A registry
that hands out fresh identifiers on restart makes every message written before
the restart unreadable, and does it silently — the bytes still parse as *a*
schema, just not the right one.

| | |
|---|---|
| `InMemorySchemaRegistry` | Tests and single-process demos. Forgets everything on restart |
| `JdbcSchemaRegistry` | A table, in `acemq-amqp-patterns`. Survives restarts and is shared between replicas |

`JdbcSchemaRegistry` caches both directions in memory and never invalidates,
because neither answer can change: an identifier stands for one schema forever,
and a schema keeps the identifier it was given. Registration is idempotent by
content — the same schema text registered from eight replicas at once yields one
row and one identifier.

It is not Confluent's registry and does not try to be: no compatibility checking,
no versioning UI, no REST API. It remembers which integer means which schema,
which is the part the wire format depends on. Where a Confluent registry is
already running, point the codec at that instead.

`createSchemaIfAbsent()` is for development. In production the table belongs in
whatever migration tool already owns the schema.

## Your own format

```java
public final class CsvCodec implements Codec {
    public byte[] encode(Object payload) { ... }
    public <T> T decode(byte[] body, Class<T> type) { ... }
    public String contentType() { return "text/csv"; }
    public boolean canDecode(String contentType) { ... }
}

mq.publisher("reports", "daily", Report.class).as(new CsvCodec());
```

Register it for the read side by publishing a `CodecProvider` through
`ServiceLoader`, and consumers will accept it alongside the built-ins.
