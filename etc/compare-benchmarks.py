#!/usr/bin/env python3
"""Compare a JMH result against a stored baseline, measured on the same machine.

A benchmark nobody compares is a number nobody reads. This turns a run into a
verdict: it prints the change per benchmark and fails when one of them regresses
past the threshold by more than the measurement's own uncertainty.

Two rules, both learned from this gate being red every night from 27 August:

*Never across machines.* The baseline is chosen by the caller and is expected to
be per environment -- the workflow passes benchmarks/results/baseline-<runner>-
<jdk>.json. The JDK and VM recorded inside the two files are checked as well, and
a mismatch is refused rather than reported as a regression, because the
difference it would report is the hardware. A missing baseline for this
environment is not a failure: it is a baseline nobody has recorded yet.

*Never on the means alone.* JMH prints a confidence interval next to every score
and the old comparison ignored it, so a 15% threshold on a measurement with +/-8%
intervals fired on noise. A benchmark regresses here only when its whole interval
is above the threshold.

Usage:
    compare-benchmarks.py <current.json> <baseline.json> [max-regression-percent]

A benchmark absent from the baseline is reported as new and does not fail the
run. A benchmark absent from the current results does fail it: silently dropping
a benchmark is how coverage of a hot path disappears.

Exit codes:
    0  no regression, or nothing comparable to compare against
    1  a benchmark regressed, or one went missing
    2  the two files were measured on different environments
"""

import sys

import jmh


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2

    current = jmh.load(sys.argv[1])
    try:
        baseline = jmh.load(sys.argv[2])
    except FileNotFoundError:
        print("benchmarks: no baseline at {} yet.".format(sys.argv[2]))
        print("benchmarks: recording this run as the first one for {}.".format(
            jmh.environment(current)))
        for name in sorted(current):
            print("  {:<44} {}".format(name, current[name].format()))
        return 0

    here, there = jmh.environment(current), jmh.environment(baseline)
    if here != there:
        print("benchmarks: this run was measured on {}".format(here), file=sys.stderr)
        print("benchmarks: the baseline was measured on {}".format(there), file=sys.stderr)
        print("benchmarks: refusing to compare across environments -- the difference "
              "reported would be the machine, not the code.", file=sys.stderr)
        return 2

    limit = float(sys.argv[3]) if len(sys.argv) > 3 else 15.0
    regressions, missing, inconclusive = [], [], []

    print("benchmarks: {} against a baseline measured on the same environment ({})".format(
        sys.argv[1], here))
    print("")
    print("  {:<40} {:>22} {:>22} {:>18}".format("benchmark", "baseline", "current", "change"))
    for name in sorted(set(baseline) | set(current)):
        if name not in current:
            missing.append(name)
            print("  {:<40} {:>22} {:>22} {:>18}".format(
                name, baseline[name].format().strip(), "MISSING", "-"))
            continue
        if name not in baseline:
            print("  {:<40} {:>22} {:>22} {:>18}".format(
                name, "new", current[name].format().strip(), "-"))
            continue

        change, error = jmh.relative_change(current[name], baseline[name])
        outcome = jmh.verdict(change, error, limit)
        if error is None:
            shown = "{:+.1f}% (no interval)".format(change)
        else:
            shown = "{:+.1f}% +/-{:.1f}".format(change, error)
        marker = {"over": "  <-- regression", "inconclusive": "  (inconclusive)",
                  "unmeasured": "  (no interval)", "within": ""}[outcome]
        print("  {:<40} {:>22} {:>22} {:>18}{}".format(
            name, baseline[name].format().strip(), current[name].format().strip(), shown, marker))

        if outcome == "over":
            regressions.append((name, change, error))
        elif outcome in ("inconclusive", "unmeasured"):
            inconclusive.append(name)

    print("")
    if missing:
        print("benchmarks: {} benchmark(s) present in the baseline but absent now: {}".format(
            len(missing), ", ".join(missing)), file=sys.stderr)
    for name, change, error in regressions:
        print("benchmarks: {} regressed {:+.1f}% +/-{:.1f}, entirely above the {:.1f}% "
              "allowed".format(name, change, error, limit), file=sys.stderr)
    if inconclusive and not regressions and not missing:
        # Worth saying out loud. A run where everything is inconclusive is a run
        # that proved nothing, and it should not read as a clean pass.
        print("benchmarks: {} benchmark(s) moved by an amount this run cannot resolve: {}".format(
            len(inconclusive), ", ".join(inconclusive)))
        print("benchmarks: not failing on them. Raise @Fork or @Measurement to narrow the "
              "intervals if these matter.")

    return 1 if (regressions or missing) else 0


if __name__ == "__main__":
    sys.exit(main())
