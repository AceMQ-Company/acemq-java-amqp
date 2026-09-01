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

## What is not built yet

Two things this documentation would otherwise imply, stated plainly because
guessing wrong about security is expensive:

- **There is no certificate generator.** The plan is a module that mints
  self-signed development certificates carrying the marker above. It does not
  exist. The JDK cannot generate a certificate on its own — `keytool` and
  `KeyPairGenerator` produce key pairs, and the signing needs something like
  BouncyCastle — so this is a real dependency decision rather than a few lines
  that were forgotten.
- **There is no payload encryption.** Transport security protects the message in
  flight; it does nothing about a message at rest in a queue that an operator can
  read. If your payloads need to be opaque to the broker, encrypt them in your
  own code today, or wait for this.

Until the generator exists, make development certificates with the tools you
already have:

```bash
# A self-signed certificate and a keystore RabbitMQ can use.
keytool -genkeypair -alias rabbitmq -keyalg RSA -keysize 4096 \
        -dname "CN=localhost, OU=ACEMQ DEVELOPMENT ONLY - DO NOT TRUST" \
        -validity 90 -keystore dev-keystore.p12 -storetype PKCS12 \
        -storepass changeit

keytool -exportcert -alias rabbitmq -keystore dev-keystore.p12 \
        -storepass changeit -rfc -file dev-cert.pem
```

Put the marker in the subject as shown. It costs nothing and makes the certificate
refuse to work anywhere that has not explicitly opted in.

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
