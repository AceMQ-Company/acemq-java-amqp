# Releasing

Artifacts are published to a **Git-hosted Maven repository**:
[AceMQ-Company/maven](https://github.com/AceMQ-Company/maven), served over GitHub
Pages at <https://acemq-company.github.io/maven/>. Consumers add one
`<repository>` element and need no credentials.

## Why not Maven Central

Central is permanent: an artifact published there can never be removed, replaced
or altered. That is the wrong trade before 1.0, while coordinates and API shape
are still moving — here a bad release can be deleted.

The cost is real and belongs in the open: because these artifacts are not on
Central, any library that depends on AceMQ obliges *its* consumers to add this
repository too. That is fine for applications and awkward for libraries, and it
is the reason 1.0 moves to Central.

Central also means Javadoc is self-hosted rather than served by javadoc.io, so
the documentation site is part of the release rather than a nicety.

## Cutting a release

1. **Check the changelog.** Move `[Unreleased]` to the new version with today's
   date, and start a fresh `[Unreleased]`.
2. **Update the version in prose** — the README badge and status paragraph,
   `docs/getting-started.md`, `docs/testing.md`, and the landing page in the
   `maven` repository. These are what people copy; a stale one sends them to a
   version that does not exist.
3. **Tag it.** `git tag v0.1.0 && git push origin v0.1.0`.

The tag triggers `.github/workflows/release.yml`, which runs the full suite
including integration tests, deploys into a checkout of the `maven` repository,
and pushes.

### Publishing by hand

The workflow needs a credential with write access to `AceMQ-Company/maven`
(`MAVEN_REPO_TOKEN`, or a deploy key once org policy allows one). Until that is
in place, publish from a machine that can already push there:

```bash
DRY_RUN=1 ./scripts/publish-maven-repo.sh 0.1.0    # build and stage, push nothing
./scripts/publish-maven-repo.sh 0.1.0              # publish
```

That script lives outside this repository, in the working folder alongside it.
It builds a copy at the requested version, so the working tree is never left
holding a release version that has to be reverted.

## What a release contains

Twelve modules, each with its main jar, `-sources` and `-javadoc`. Checksums are
written for everything. Signing is opt-in (`ACEMQ_SIGN=1`) and off by default,
because unsigned artifacts are ordinary for a repository like this one and a
build that fails for want of a GPG key is not.

## Verifying a release

Resolve it from an empty local repository, which is the only check that proves a
consumer can actually use it:

```bash
mvn -q -Dmaven.repo.local=/tmp/verify-m2 dependency:get \
  -DremoteRepositories=https://acemq-company.github.io/maven/ \
  -Dartifact=org.acemq:acemq-amqp-core:0.1.0
```

GitHub Pages takes a minute or two to serve newly pushed files.

## After a release

Bump the development version:

```bash
mvn versions:set -DnewVersion=0.2.0-SNAPSHOT -DgenerateBackupPoms=false
```

## Rules

- **Never publish a `SNAPSHOT`** to the release repository. Both the script and
  the workflow refuse: snapshots write timestamped filenames and metadata that a
  static repository cannot resolve reliably. Snapshots go to GitHub Packages,
  which the `main` build already does.
- **Never rewrite a published version.** Deleting one is possible here and is
  occasionally right; quietly replacing the contents of a version somebody has
  already resolved is not.
- **Release from `main`, green.** The workflow re-runs the whole suite anyway; a
  release is the one moment where waiting for it is obviously worth it.
