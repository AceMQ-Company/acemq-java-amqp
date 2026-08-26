#!/usr/bin/env python3
"""Compare a JMH result against a stored baseline.

A benchmark nobody compares is a number nobody reads. This turns a run into a
verdict: it prints the change per benchmark and fails when any of them regresses
by more than the allowed percentage.

Usage:
    compare-benchmarks.py <current.json> <baseline.json> [max-regression-percent]

A benchmark absent from the baseline is reported as new and does not fail the
run. A benchmark absent from the current results does fail it: silently dropping
a benchmark is how coverage of a hot path disappears.
"""

import json
import sys


def load(path):
    with open(path) as handle:
        return {r["benchmark"].split(".")[-1]: r["primaryMetric"]["score"] for r in json.load(handle)}


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2

    current = load(sys.argv[1])
    try:
        baseline = load(sys.argv[2])
    except FileNotFoundError:
        print("no baseline yet; recording this run as the first one")
        for name, score in sorted(current.items()):
            print("  {:<44} {:8.3f}".format(name, score))
        return 0

    limit = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
    regressions, missing = [], []

    print("{:<44} {:>10} {:>10} {:>9}".format("benchmark", "baseline", "current", "change"))
    for name in sorted(set(baseline) | set(current)):
        if name not in current:
            missing.append(name)
            print("  {:<42} {:>10.3f} {:>10} {:>9}".format(name, baseline[name], "MISSING", "-"))
            continue
        if name not in baseline:
            print("  {:<42} {:>10} {:>10.3f} {:>9}".format(name, "new", current[name], "-"))
            continue

        # Lower is better for average time, which is the mode these run in.
        change = 100.0 * (current[name] - baseline[name]) / baseline[name]
        flag = "  <-- regression" if change > limit else ""
        print("  {:<42} {:>10.3f} {:>10.3f} {:>8.1f}%{}".format(name, baseline[name], current[name], change, flag))
        if change > limit:
            regressions.append((name, change))

    print("")
    if missing:
        print("benchmarks: {} benchmark(s) present in the baseline but absent now: {}".format(
            len(missing), ", ".join(missing)), file=sys.stderr)
    for name, change in regressions:
        print("benchmarks: {} regressed by {:.1f}%, over the {:.1f}% allowed".format(
            name, change, limit), file=sys.stderr)

    return 1 if (regressions or missing) else 0


if __name__ == "__main__":
    sys.exit(main())
