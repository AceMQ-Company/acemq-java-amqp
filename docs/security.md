# Security

The defaults are strict, and the ways round them are named rather than implied.

## The rule

**The URL decides whether the connection is encrypted. The policy decides how
strictly the certificate is checked.** These are separate on purpose: an
`amqp://` URL that quietly upgraded itself to TLS would be a connection nobody
could reason about, and a policy that silently downgraded `amqps://` would be
worse.

```java
AceMq.connect("amqp://localhost");     // plaintext, and says so
AceMq.connect("amqps://broker:5671");  // TLS, certificate verified, hostname checked
```

`amqps://` combined with `Security.disabled()` is **refused**, not honoured. If
you asked for TLS and then asked for no verification, one of the two was a
mistake and the library will not guess which.

## Policies

```java
ConnectionConfig.url("amqps://broker:5671")
        .security(Security.required())          // the default
        .build();
```

| | What it does | Use for |
|---|---|---|
| `Security.required()` | Verify the certificate chain and the hostname | **Default.** Production |
| `Security.fromKeystore(dir)` | As above, against your own trust store | Private or internal CAs |
| `Security.insecure()` | Encrypt, but do not verify | Diagnosis. Never production |
| `Security.disabled()` | No TLS at all | Local development |

`keystorePassword(...)` and `allowDevelopmentCertificates()` refine those.

## Credentials

```java
ConnectionConfig.url("amqps://broker:5671")
        .security(Security.required().withCredentials(
                () -> Credentials.of("orders-service", vault.currentPassword())))
        .build();
```

`CredentialsProvider` is consulted **on every connection**, not once at start-up.
That is what makes rotation work: a password changed in your secret store is
picked up by the next reconnect rather than at the next deployment. Credentials
are held as `char[]` and never appear in `toString()`.

`Credentials.token(...)` exists for OAuth-style brokers.

## Development certificates are refused by default

A certificate whose subject or issuer carries the marker

```
ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
```

is rejected however the trust store is configured, unless you call
`allowDevelopmentCertificates()`. A development certificate that reaches
production is the kind of mistake that looks like it is working, so the marker is
checked at the point where it would otherwise do damage.

## Development certificates, in one command

TLS on a laptop is the thing that gets postponed for months, so it takes one
command. First tell Maven where the plugin lives — this is a
`<pluginRepositories>` entry, **not** `<repositories>`: the first says where
dependencies come from, the second where plugins do, and a plugin declared in the
wrong one is looked for in Maven Central alone.

```xml
<pluginRepositories>
  <pluginRepository>
    <id>acemq</id>
    <url>https://acemq-company.github.io/maven/</url>
  </pluginRepository>
</pluginRepositories>
```

Then:

```bash
mvn org.acemq:acemq-security-dev:0.2.1:certs -Dbroker=localhost -Dout=./certs
```

The [examples repository](https://github.com/AceMQ-Company/acemq-java-amqp-examples)
wires this into a profile, so there it is just `mvn -Pgencert`.

That writes a throwaway certificate authority, a broker certificate valid for
the host you named, a client key pair, the two keystores
`Security.fromKeystore(...)` reads, and a matching `rabbitmq.conf`:

```
ca.crt          the authority to trust
server.crt/.key for the broker
keystore.p12    this client's key pair
truststore.p12  the authority
rabbitmq.conf   mount at /etc/rabbitmq/rabbitmq.conf
```

Then connect:

```java
AceMq.connect(ConnectionConfig.url("amqps://localhost:5671")
        .security(Security.fromKeystore(Path.of("./certs"))
                .allowDevelopmentCertificates())
        .build());
```

No password needed: the generator writes the stores with the same default
`fromKeystore` assumes. Anything real passes `keystorePassword(...)` with a value
from a secret store.

Options: `-Ddays=30` (the default, deliberately short), `-Dpassword=...` (at least six
characters — `keytool` refuses to create a PKCS12 store with fewer),
`-DskipBrokerConfig=true`.

Three things make these safe to lose:

- Every certificate carries `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` in its
  subject, and the library refuses such a certificate unless you call
  `allowDevelopmentCertificates()`.
- They expire in thirty days by default. A development certificate that never
  expires outlives the reason it was created.
- The goal **refuses to run when `ACEMQ_ENV` starts with `prod`**. That is not a
  security control — anyone can unset an environment variable — but the mistake
  it catches is not malice. It is a deployment script that ran a development
  command because somebody copied a README.

The signing key is written next to the certificates and is not a secret. That is
the point of the marker: these fail closed anywhere that has not explicitly
opted in.

## What is still not built

**There is no payload encryption.** Transport security protects the message in
flight and does nothing about a message at rest in a queue an operator can read.
If your payloads need to be opaque to the broker, encrypt them in your own code
today.

## Production checklist

- `amqps://`, and `Security.required()` — which is the default, so this is really
  a check that nothing downgraded it.
- Credentials from a secret store through a `CredentialsProvider`, not from a URL
  or a properties file. A password in a URL ends up in logs, in `ps` output, and
  in exception messages.
- A user per service, with permissions limited to the exchanges and queues that
  service uses. RabbitMQ's default `guest` account cannot connect remotely; do
  not "fix" that.
- `allowDevelopmentCertificates()` appears nowhere in the deployment.
- Payload encryption in your own code if the broker's operators should not be
  able to read the messages.

---

**Need this reviewed?** AceMQ Enterprise support covers TLS configuration,
certificate rotation, per-service permission design, and the RabbitMQ-side
hardening this page cannot do for you.
