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

**The minor number moves with each release: `0.3.0`, `0.4.0`, `0.5.0`, `0.6.0`,
`0.7.0`.** The patch number is for a fix cut on top of a release that is already
out, which is what `0.2.1` through `0.2.10` were; it has not been needed since.

This used to say the opposite — stay on `0.2.x` until 1.0, so that the version
number never implied the API had stabilised before it had. That policy was
abandoned at `0.3.0` and the page was not updated, so it spent four releases
telling the next releaser to cut a patch. What it was protecting against is
covered by saying the thing outright instead: **while the version is `0.x` the
public API may change in any release**, which is what the changelog says at the
top and what semver means by leaving `0.y.z` outside its compatibility
guarantees. A moving minor number is the honest signal that a release did change
things.

The corollary is unchanged: **anything depending on AceMQ before 1.0 should pin
an exact version**, not a range.

The five libraries release together on one number, so the minor here moves when
the others move, whether or not this repository had the headline change.

## Cutting a release

1. **Check the changelog.** Move `[Unreleased]` to the new version with today's
   date, and start a fresh `[Unreleased]`.
2. **Nothing to update by hand.** The release rewrites the version in the guide,
   the README and the Maven repository's landing page. Those had all drifted
   while it was a manual step — the landing page by two releases — which is
   worse than saying nothing, because a reader copies it.

   **The pom is not edited either.** The version comes from the tag: the
   workflow strips the leading `v`, refuses anything that is not a plain version
   number, and runs `versions:set` itself before deploying. Nothing committed
   here names the version being released, so there is no pom change to forget
   and no way for the tag and the artifacts to disagree.

   One thing the rewrite cannot tell apart: a version somebody is meant to
   **copy** and a sentence **about** a past release. `set-documented-version.sh`
   replaces every three-segment `0.x.y` in `docs/*.md` and `README.md`, which
   would turn "since 0.5 all five write both" into a claim about the release
   being cut. Write history with two segments — `0.5`, `0.6` — which the pattern
   does not match, and keep three segments for coordinates.
3. **Commit everything**, and check `git status` is clean.
4. **Pull first.** The previous release's `document` and `landing-page` jobs push
   to `main`, so `main` has almost certainly moved since you started. Tagging
   without pulling gives a tag whose commit is not on `main` — the release still
   builds correctly, because it builds from the tag, but the released commit is
   not in the branch history. This has happened three times; `git pull --rebase`
   before tagging costs nothing.

   If it happens anyway, **merge rather than rebase**: rebasing changes the
   commit the tag points at, and a release tag that is not reachable from `main`
   is worse than a merge commit.
5. **Tag it.** `git tag -a v0.7.0 -m 0.7.0 && git push origin v0.7.0`.
   Annotated: a bare `git tag -m` is rejected, and `git tag` alone opens an
   editor that fails in a non-interactive shell.

That is the whole release. The tag triggers
`.github/workflows/release.yml`, which runs the full suite including integration
tests, publishes into the `maven` repository, **verifies the new version
resolves from an empty local repository**, and announces it in Slack.

### Release from the tag, not from a working tree

`0.2.1` was published by hand and went out without the fix it was named for: the
commit had not been made, so the artifacts did not match the repository, and
nothing noticed until CI in another repository failed. The workflow builds from
the tag, which makes that impossible.

**There is no working-tree fallback, and this page used to claim there was** — a
`scripts/publish-maven-repo.sh` that has never existed in this repository.
Anyone reaching for it during an incident would have spent the search finding
that out.

What the workflow offers instead is `workflow_dispatch`, which takes the version
as an input and builds from whichever ref you run it on. That keeps the property
the hand-publish lost: the artifacts are built from a commit that is pushed, so
what is published and what is readable are the same thing.

```bash
gh workflow run release.yml --ref main -f version=0.7.0
```

Whatever route it took, then check the published artifact actually contains what
you released — downloading the jar and looking is thirty seconds, and is the
step whose absence cost a version.

## What a release contains

Fifteen modules, each with its main jar, `-sources` and `-javadoc`: every module
in the root `<modules>` list except `acemq-amqp-coverage`, which sets
`maven.deploy.skip`, as do the profile-guarded `acemq-amqp-benchmarks` and
`acemq-amqp-native`. The count said twelve for several releases after the list
grew, which is the sort of number worth deriving rather than remembering.

Checksums are written for everything. Signing is opt-in (`ACEMQ_SIGN=1`) and off
by default, because unsigned artifacts are ordinary for a repository like this
one and a build that fails for want of a GPG key is not.

## Verifying a release

Resolve it from an empty local repository, which is the only check that proves a
consumer can actually use it:

```bash
mvn -q -Dmaven.repo.local=/tmp/verify-m2 dependency:get \
  -DremoteRepositories=https://acemq-company.github.io/maven/ \
  -Dartifact=org.acemq:acemq-amqp-core:0.7.0
```

GitHub Pages takes a minute or two to serve newly pushed files.

### The URLs redirect

The organisation site carries the custom domain `acemq.org`, and GitHub redirects
every project page beneath it. So
`https://acemq-company.github.io/maven/...` answers **301** to
`https://acemq.org/maven/...`, whatever the `maven` repository's own Pages
settings say.

Nothing is broken by this — Maven follows the redirect, and the documented
`<repository>` URL keeps working. It did cost one release: the verification step
compared the 301 against 200 and reported `0.2.4` as never published, after
publishing it correctly. That step now uses `curl -L`. Anything else that checks
these URLs by hand needs to as well.

### What Slack hears, and what it does not

**A release that fails before publishing is not announced.** Nothing reached the
Maven repository, no version exists, and nobody can resolve anything they could
not resolve before — announcing that as "release FAILED" puts a red message in
the channel about an event that did not happen. A channel that cries wolf is one
people stop reading, and that costs the announcement that matters. The failure is
still a red tag build in Actions, and whoever pushed the tag is watching it.

**Everything from the push onwards is announced, success or failure.** A version
that is published but whose release did not finish is the genuinely dangerous
state: consumers can resolve it while the documentation and the landing page
still name the previous one. That is worth interrupting someone for.

That one leads with :construction: rather than a red dot. The status underneath
still reads `failure`, because that is what the run was — but the icon is about
what the reader should *do*, and "usable and unfinished" is a different
instruction from "broken". `slack-notify.yml` takes an optional `icon` input for
exactly this.

The switch is the `published` output of the `publish` job, set by the step that
pushes to the Maven repository. Job outputs survive the job failing — verified
with a throwaway workflow rather than assumed, because guessing wrong here means
silence on the one failure that matters.

### A failed verification skips the rest of the release

`document` and `landing-page` only run when `publish` succeeds. That is
deliberate — a version that cannot be resolved should not be advertised — but it
means a false failure leaves the artifacts published and every page still naming
the previous version. If that happens, run the two steps by hand rather than
re-tagging: re-releasing would rewrite a published version, which is the one
thing this repository does not do.

```bash
.github/scripts/set-documented-version.sh 0.7.0   # then commit and push
```

and edit the version on the organisation landing page the same way.

## After a release

**Nothing has to be bumped for the next release to work**, because the release
takes its version from the tag rather than from the pom. The snapshot version in
the pom names one thing only: the snapshots `ci.yml`'s `publish-snapshot` job
pushes to GitHub Packages. It has been left behind before — it read
`0.5.0-SNAPSHOT` while `0.6.0` was the current release — which misleads anyone
resolving a snapshot and breaks nothing else.

Less than it used to, in fact: `publish-snapshot` still guards on
`github.event_name == 'push'`, and `ci.yml` no longer has a push trigger, so
nothing publishes a snapshot at all at the moment.

If you want the snapshots named after the version being worked towards:

```bash
mvn versions:set -DnewVersion=0.8.0-SNAPSHOT -DgenerateBackupPoms=false
```

## Credentials the release uses

The release writes to three repositories: this one, `maven`, and
`AceMQ-Company.github.io`. The built-in `GITHUB_TOKEN` covers this one; the
other two each have a **deploy key**, held here as `MAVEN_REPO_DEPLOY_KEY` and
`LANDING_PAGE_DEPLOY_KEY`.

A deploy key writes to exactly one repository and nothing else. That is the
whole reason for them. What they replaced was a personal access token carrying
`repo`, `admin:org` and `delete_repo` across every repository its owner could
reach — to push two commits. Anyone able to add a step to a workflow in this
repository could read it, and it would have taken the organisation with it.

They also belong to the repository rather than to a person, so they survive
whoever made them leaving, and they do not expire — a release cannot fail
because a token quietly aged out.

To rotate one:

```bash
ssh-keygen -t ed25519 -N '' -C 'acemq-java-amqp release -> maven' -f /tmp/k
gh api repos/AceMQ-Company/maven/keys -X POST \
  -f title='acemq-java-amqp release' -f key="$(cat /tmp/k.pub)" -F read_only=false
gh secret set MAVEN_REPO_DEPLOY_KEY -R AceMQ-Company/acemq-java-amqp < /tmp/k
rm /tmp/k /tmp/k.pub          # and delete the old key from the target repository
```

Deploy keys are an organisation-level permission
(`deploy_keys_enabled_for_repositories`), off by default on a new organisation
and enabled here.

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

One organisation secret would replace all three:

```bash
gh secret set SLACK_DELIVERY_WEBHOOK --org AceMQ-Company --visibility all
```

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

  Green is no longer something that happens on its own: `ci.yml` runs on
  `workflow_dispatch` only, so a commit starts nothing. Before tagging, either
  run it from the Actions tab or run the same checks locally —
  `mvn clean verify` on **JDK 21**, which is the only JDK the NullAway and
  ErrorProne profile is bound to and therefore the only one where the static
  analysis gate actually fires.
