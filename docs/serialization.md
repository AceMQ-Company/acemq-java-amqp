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
| Avro | `acemq-amqp-codec-avro` |
| Protobuf | `acemq-amqp-codec-protobuf` |

Codecs are found by `ServiceLoader`, so adding the dependency is the whole
installation. Asking for a format whose module is absent fails immediately, by
name, rather than at the first message.

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
