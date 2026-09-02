# Tutorials

Step by step, in order, each one ending with something that runs.

The [guide](index.html) explains how a thing works and why it is that way. These
are the other shape: start with nothing, finish with a working service, and
understand what you typed by the end rather than before the beginning.

| | | |
|---|---|---|
| 1 | [Your first message](tutorial-first-message.html) | Connect, declare, publish, consume. No broker needed | 10 min |
| 2 | [Surviving failure](tutorial-surviving-failure.html) | Retries that do not block, dead letters, and replaying them | 20 min |
| 3 | [Never processing twice](tutorial-exactly-once.html) | Idempotency, the outbox, and why "exactly once" is a lie | 25 min |
| 4 | [Seeing what happens](tutorial-observability.html) | Traces and metrics, and reading them when something is wrong | 20 min |

Each builds on the one before it, and each is a single file you can paste into a
scratch project. Nothing is left as an exercise.

## Before you start

```xml
<repositories>
  <repository>
    <id>acemq</id>
    <url>https://acemq-company.github.io/maven/</url>
  </repository>
</repositories>

<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-core</artifactId>
  <version>0.2.9</version>
</dependency>
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-test</artifactId>
  <version>0.2.9</version>
</dependency>
```

Java 11 or newer. Tutorials 1 and 2 need no broker at all — `acemq-amqp-test`
provides an in-memory one behind a `memory://` URL that implements the same
interface a real broker does. Tutorials 3 and 4 use Docker.

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## If you would rather read code

The [examples repository](https://github.com/AceMQ-Company/acemq-java-amqp-examples)
has 26 runnable programs plus a full enterprise application, each verified by CI
on every commit. Tutorials teach; examples demonstrate. Start here, go there when
you want to see a whole system rather than one idea.

## Other languages

Only Java today. Ports, and a Spring Boot guide, follow the same numbering when
they arrive, so tutorial 3 will teach the same thing in every language.
