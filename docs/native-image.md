# GraalVM native image

AceMQ works in a native image, and **needs no configuration of its own** to do
so. What you have to configure is your own message types.

```bash
mvn -Pnative clean verify   # in this repository: builds an image and runs it
```

## What you have to do

**Register the types you send and receive.** Jackson reads them by reflection and
no call site names the accessors, so a closed-world compiler removes what nothing
appears to use. The image builds cleanly and then fails on the first publish:

```
No serializer found for class com.example.Order and no properties discovered to
create BeanSerializer ... This appears to be a native image, in which case you
may need to configure reflection for the class that is to be serialized
```

`src/main/resources/META-INF/native-image/com.example/app/reflect-config.json`:

```json
[
  {
    "name": "com.example.Order",
    "allDeclaredConstructors": true,
    "allDeclaredMethods": true,
    "allDeclaredFields": true
  }
]
```

On Spring Boot or Quarkus this is already handled — `@RegisterForReflection`,
`RuntimeHints`, or the framework's own scanning. Elsewhere, the
[tracing agent](https://www.graalvm.org/latest/reference-manual/native-image/metadata/AutomaticMetadataCollection/)
writes the file for you from a JVM run:

```bash
java -agentlib:native-image-agent=config-output-dir=src/main/resources/META-INF/native-image/com.example/app \
     -jar target/app.jar
```

Exercise every message type while the agent is attached. A type that is never
serialised during the run is a type that is missing from the file.

## What you do not have to do

Verified, on **GraalVM CE 21 and 25**, by building an image and running it — not
by reasoning about it:

| | |
|---|---|
| Codec discovery | `Codecs.byName("json")`, `byName("yaml")` — `ServiceLoader` finds them |
| In-memory transport | Publish and consume, end to end |
| RabbitMQ transport | Connect, declare, publish with confirms, consume, acknowledge |
| Payload encryption | `SecureRandom` and AES-GCM through the JCA |
| Schema registry | Including the `.sql` file it reads out of the jar |
| TLS | Providers, PKCS12 keystores, the trust manager factory |

None of that needed a `reflect-config.json`, a `resource-config.json`, or a
`native-image.properties` from AceMQ. The library ships no reachability metadata
because, so far, it has needed none — shipping configuration that testing shows
is unnecessary is a promise to maintain something nobody reads.

If you hit a case that does need it, that is a bug worth reporting: the fix
belongs in the library's jars, not in every application's.

## Keeping it true

`mvn -Pnative clean verify` builds `acemq-amqp-native` — a real image running
real checks — and a nightly job runs it. The claim above is only worth making if
something keeps checking it, and a dependency bump is what quietly breaks it.

Two things make it a gate rather than decoration:

- `--no-fallback`. A fallback image "works" by bundling a JVM, which would let
  the test pass on the day ahead-of-time compilation stopped.
- `clean`. Without it the plugin can reuse the previous binary and report a pass
  for code that was never compiled. This was not hypothetical — it happened while
  the test was being written.

The smoke test needs no broker, so it runs anywhere in about a second once built.
The RabbitMQ transport over a real socket is covered by the integration suite on
the JVM.

## Related

- [Serialization](serialization.html) — which types cross the wire, and so which
  ones need registering
- [Security](security.html) — TLS, which works in an image unchanged
