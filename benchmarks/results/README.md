# Benchmark results

Two kinds of file live here, and the difference between them is the whole point.

| File | What it is |
|---|---|
| `current.json` | The run that just happened. Never committed — it is uploaded as a workflow artifact and kept for 90 days |
| `baseline-<os>-<arch>-jdk<n>.json` | What that environment measured last time somebody chose to record it |

## Why the baseline name carries the machine

A JMH score is a fact about a machine running a build, not about the code alone.
The first version of this directory held one `baseline.json`, measured on a
laptop on 26 August, and the nightly compared it against `ubuntu-latest` every
night from 27 August. What that comparison reported was the hardware, and it
filed an issue about it eight times.

`etc/compare-benchmarks.py` now refuses a baseline whose JDK and VM do not match
the current run, and the workflow names the file after the runner, so putting the
wrong one in front of it takes deliberate effort rather than a default.

A missing baseline for an environment is not a failure. It is a baseline nobody
has recorded yet, and the run says so and passes.

## Recording one

Run the `nightly-benchmarks` workflow by hand with `update-baseline: true`. A
separate job — the only one in that workflow with write access, and one that runs
no code from this repository — commits the result under the name for that
environment.

Do it when a change is expected to move the numbers, in or near the pull request
that causes it, so the baseline and the reason for it arrive together.

## Reading the numbers

Every score here comes with a `scoreError`: the half-width of JMH's 99.9%
confidence interval for the mean. Both checks in `etc/` compare intervals rather
than means, and fail only when a whole interval is past its threshold. A run
whose interval straddles the threshold is reported as inconclusive and passes —
`etc/test-benchmark-checks.py` pins that behaviour, including the 3 September run
that was reported as `+6.8% FAIL` on a measurement that could not tell 0% from
13%.
