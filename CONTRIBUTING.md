# Contributing

Thank you for considering a contribution.

## The gate model

Every change moves through five gates. None may be skipped; a small fix simply
compresses the design gate into a paragraph on the issue.

| Gate | What must exist to leave it |
|---|---|
| **Design** | A design note on the issue, or an ADR in `docs/adr/` for anything touching the public API, the SPI, the message envelope, or broker semantics. Public API signatures reviewed **before** implementation. Telemetry (span names, metric names, tags) and extension points named up front. |
| **Code** | Implementation, telemetry, replaceable seams, Javadoc on every public symbol including failure modes and thread safety, and a `CHANGELOG.md` entry in the same pull request. |
| **Test** | Unit tests (fast, hermetic, against the in-memory transport), smoke tests, and integration tests against real brokers via Testcontainers or Docker Compose. Telemetry is asserted, not assumed. |
| **Benchmark** | Benchmarks for any changed hot path, run with telemetry both on and off. AceMQ stays within 5 % throughput and 200 µs added p99 latency of the raw underlying client; exceeding that needs a written waiver in the pull request. |
| **Release** | Version compatibility check, signed artifacts, SBOM, provenance, human-written release notes. |

## Ground rules for code

- **Telemetry is not optional.** Every publish and delivery emits a span; counters
  and timers use the names frozen in the specification. When no telemetry provider
  is on the classpath, instrumentation must compile to no-ops.
- **Everything is replaceable.** No `final` or `sealed` type on a seam, no static
  singletons in the hot path, and every default reachable through a public
  constructor or builder so it can be wrapped rather than replaced.
- **`acemq-amqp-api` takes no third-party dependencies.** It is the module the
  other language ports are transliterated from; anything added there becomes a
  cross-language commitment.
- **No silent degradation.** If a broker lacks a capability, either apply a
  documented alternative or fail at startup naming the capability. Never pretend.
- **No `TODO` without an issue number.**

## Building

```bash
mvn clean verify        # build, test, coverage gate, format check
mvn spotless:apply      # fix formatting before committing
```

JDK 17 or newer is required to build. The published bytecode targets Java 11, so
do not use language or library features newer than 11 outside a multi-release
source set.

## Commits and pull requests

- [Conventional Commits](https://www.conventionalcommits.org/): `feat(core): ...`,
  `fix(transport-rabbitmq): ...`, `docs: ...`, `test: ...`, `chore: ...`.
- Run `git config core.hooksPath .githooks` once after cloning so the local
  checks run.
- Sign your commits.
- Keep pull requests focused, link the issue, and state whether the change
  affects the cross-language contract.

## Authorship policy

Commits, pull requests, issues, documentation, and release notes in this
repository must not contain automated-tool authorship attribution of any kind —
no `Co-Authored-By` trailers for tooling, no "generated with" footers, no
assistant credits. A required status check enforces this on every pull request,
and a local `commit-msg` hook catches it earlier. Use your own name and email.

## Reporting security issues

Please do not open a public issue. See [SECURITY.md](SECURITY.md).
