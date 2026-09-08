# The cross-language fixtures

Two JSON files that say what every AceMQ library must do. Java generates them;
Go, .NET, Python and Ruby carry a copy and assert against it.

| File | What it pins |
| --- | --- |
| `envelope-fixtures.json` | the headers an AceMQ publish puts on the wire |
| `contract-fixtures.json` | retry schedules, jitter bounds, the consumer/broker threshold, queue naming, the rung argument table, the declared topology, and queue type defaults |

They are **generated, never written**. Two implementations agreeing with the
same prose is not interoperability; agreeing with the same bytes is. Every
number in `contract-fixtures.json` is computed by calling the library —
`RetryPolicy.schedule()`, `RetryTopology.rungArguments(...)`, `Topology`
itself — so a change to the arithmetic changes the file, and a file that no
longer matches fails the build.

The reason this exists is concrete. `RetryPolicy.exponential` multiplied by five
and jittered by ten percent in Java while the other four libraries doubled and
jittered by twenty, for ten releases. `exponential(5, 1s, 1m)` produced
`1s, 5s, 25s, 60s` here and `1s, 2s, 4s, 8s` everywhere else. Nothing caught it,
because each library tested its own arithmetic against its own expectations.

## Regenerating

```bash
mvn -pl acemq-amqp-test test -Dtest=FixtureDriftTest -Dacemq.fixtures.write=true
```

Then carry the new bytes to `acemq-go-amqp`, `acemq-dotnet-amqp`,
`acemq-python-amqp` and `acemq-ruby-amqp` **in the same change**. A fixture
updated in one repository and not the other four is worse than no fixture at
all, because it looks like agreement.

Without the property, `FixtureDriftTest` compares instead of writing, which is
what CI runs. `ContractConformanceTest` then holds this library to the file it
wrote, deriving its expectations a second way wherever there is one — the
doubling is checked by asking whether each delay is twice the one before it,
not by reading the multiplier back.

## What is not byte-reproducible, and why

`envelope-fixtures.json` has a `minimal` case: the message a caller publishes
having supplied nothing. Its identifier is therefore a fresh UUID, its
`x-acemq-first-seen` is the clock and its `x-acemq-origin` carries the hostname
of whichever machine ran the generator — the committed copy still says
`acemq@Kenshi.local`. Four lines of forty cannot be reproduced, and they are the
four the contract defines as "whatever the caller did not say".

The four ports read this file as *input* to their own round trip rather than as
literal expectations, so those values are arbitrary to them. `FixtureDriftTest`
masks exactly those four and compares everything else byte for byte, then
asserts separately that the masked values are the right kind of thing: a UUID,
a correlation equal to the id, an epoch-millisecond timestamp near now, and an
origin of the form `acemq@host`. Regeneration only rewrites the file when the
masked comparison actually differs, so a routine regeneration does not roll a
new UUID into five repositories for nothing.

## Disagreements recorded rather than resolved

- **Sub-second rung names.** Java renders `{queue}.retry.500ms` where Go,
  Python and Ruby render `{queue}.retry.0s`. Unreachable through the default
  thirty-second threshold, and only reachable at all for a policy that
  deliberately lowers the threshold below a second. Recorded in
  `naming.subSecondDisagreement`. Whichever way it is settled, it has to be
  settled in five places at once.
- **Exclusive, auto-delete and transient queues.** The Java `Topology` API
  cannot declare one, so the rule that such a queue stays classic has nothing
  to attach to here. Recorded in
  `queueTypeDefaults.durability.exclusiveAutoDeleteOrTransient` as absent
  rather than invented.
