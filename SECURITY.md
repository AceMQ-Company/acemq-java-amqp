# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do
not open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of
whether it is a vulnerability, what is affected, and a rough timeline within a week.
If a fix is warranted, we will tell you when it is released and credit you unless you
would rather we did not.

## What is in scope

Every module in this repository, and particularly `acemq-security-api`,
`acemq-security-dev`, `acemq-amqp-crypto` and `acemq-transport-rabbitmq`.

Things worth reporting even if they feel minor:

- A way to reach a broker without the verification `Security.required()` asked for,
  or with hostname verification silently not happening.
- A certificate carrying the `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` marker being
  accepted without `allowDevelopmentCertificates()`.
- Anything that renders a credential, a key, or a message body into a log line or an
  exception message. `Credentials` and `EncryptionKey` are meant to keep their
  contents out of `toString()`.
- A body that `EncryptedCodec` accepts after it has been altered, or a way to make it
  decrypt under a key the sender did not use.
- A key that stays reachable in `Keyring` after it should have been retired.
- A codec — JSON, XML, YAML, TOML, Avro or Protobuf — that can be made to construct a
  type the message names rather than the type the consumer asked for.
- A reserved `x-acemq-` header that reaches application code, or an application
  header that can impersonate one.

## What is not

- **`Security.insecure()` accepting any certificate.** That is what it is for, and it
  says so. It still refuses development-marked certificates unless they are allowed
  explicitly.
- **`Security.disabled()` using a plaintext connection.** Likewise.
- **The default development keystore password.** `acemq-dev` is published in the
  documentation on purpose; the keystores it protects are marked never to be trusted.
- **Vulnerabilities in RabbitMQ itself** — report those to Broadcom.
- **Vulnerabilities in a third-party codec library** — report those upstream; tell us
  too if this library's use of one makes it worse.
- Findings from a scanner with no demonstrated impact.

## Supported versions

Pre-1.0, only the latest release. There are no maintenance branches yet, so a fix
means a new patch version.

## What this library does not do for you

It secures the connection and, optionally, the message body. It does not manage
broker users or permissions, hold your keys for you, or decide who may publish what.
The security guide at [acemq.org](https://acemq.org/) says which is which, and ends
with a production checklist.
