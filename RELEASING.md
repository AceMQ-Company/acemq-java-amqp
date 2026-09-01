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

## Which number

**Stay on `0.2.x` until 1.0.** Releases are `0.2.1`, `0.2.2`, and so on; the minor
number does not move again until the API is settled enough to call it `1.0.0`.

That is deliberate, and it is not quite semantic versioning. A `0.2.x` release
here may change the API — the changelog has always said so ("while the version is
`0.x` the public API may change in any release"), and semver explicitly leaves
`0.y.z` outside its compatibility guarantees. What the policy buys is a version
number that stops implying the API has stabilised before it has. `0.9.0` reads as
"nearly there"; `0.2.7` reads as what it is.

The corollary: **anything depending on AceMQ before 1.0 should pin an exact
version**, not a range.

## Cutting a release

1. **Check the changelog.** Move `[Unreleased]` to the new version with today's
   date, and start a fresh `[Unreleased]`.
2. **Update the version in prose** — the README badge and status paragraph,
   `docs/getting-started.md`, `docs/testing.md`, and the landing page in the
   `maven` repository. These are what people copy; a stale one sends them to a
   version that does not exist.
3. **Commit everything**, and check `git status` is clean.
4. **Tag it.** `git tag v0.2.3 && git push origin v0.2.3`.

That is the whole release. The tag triggers
`.github/workflows/release.yml`, which runs the full suite including integration
tests, publishes into the `maven` repository, **verifies the new version
resolves from an empty local repository**, and announces it in Slack.

### Release from the tag, not from a working tree

`0.2.1` was published by hand and went out without the fix it was named for: the
commit had not been made, so the artifacts did not match the repository, and
nothing noticed until CI in another repository failed. The workflow builds from
the tag, which makes that impossible.

The script in `scripts/publish-maven-repo.sh` still exists for the case where
the workflow itself cannot run. It builds from the **working tree**, so if you
ever use it:

```bash
git status                                          # must be clean
DRY_RUN=1 ./scripts/publish-maven-repo.sh 0.2.3     # stage, push nothing
./scripts/publish-maven-repo.sh 0.2.3
```

and then check the published artifact actually contains what you released —
downloading the jar and looking is thirty seconds, and is the step whose absence
cost a version.

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
mvn versions:set -DnewVersion=0.2.2-SNAPSHOT -DgenerateBackupPoms=false
```

The next *patch*, per the policy above.

## Notifications

A release, a failed release, a documentation deploy and a broken examples build
all announce themselves in Slack, through the reusable workflow in
`.github/workflows/slack-notify.yml`. Every repository calls that one workflow,
so the formatting lives in a single place.

Each repository needs the webhook as a secret:

```bash
gh secret set SLACK_DELIVERY_WEBHOOK -R AceMQ-Company/acemq-java-amqp        --body "$SLACK_WEBHOOK_URL"
gh secret set SLACK_DELIVERY_WEBHOOK -R AceMQ-Company/acemq-java-amqp-examples --body "$SLACK_WEBHOOK_URL"
gh secret set SLACK_DELIVERY_WEBHOOK -R AceMQ-Company/AceMQ-Company.github.io  --body "$SLACK_WEBHOOK_URL"
```

One organisation secret would replace all three
(`gh secret set SLACK_DELIVERY_WEBHOOK --org AceMQ-Company --visibility all`),
but that needs organisation admin rights.

Where the secret is absent the notification step does nothing and succeeds. A
missing webhook must never fail a release that worked.

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
