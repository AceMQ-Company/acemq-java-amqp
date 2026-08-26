#!/usr/bin/env python3
"""Enforce the aggregate coverage thresholds.

JaCoCo's own check goal measures one module against that module's own execution
data. That is the wrong question here: the engine in acemq-amqp-core is exercised
almost entirely from the test kit and from the transport integration tests, so
checked in isolation it reports close to nothing. This reads the aggregated
report instead, which counts every module's tests against every module's classes.

Usage:
    check-coverage.py <jacoco.csv> <min-line-percent> <min-branch-percent>

Exits non-zero, and says which package is responsible, when a threshold is missed.
"""

import csv
import sys
from collections import defaultdict


def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2

    path, min_line, min_branch = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])

    try:
        with open(path, newline="") as handle:
            rows = list(csv.DictReader(handle))
    except FileNotFoundError:
        print("coverage: no aggregate report at {}".format(path), file=sys.stderr)
        print("coverage: run the whole reactor; a single-module build has nothing to aggregate", file=sys.stderr)
        return 1

    if not rows:
        # An empty report means the aggregation silently produced nothing, which
        # would otherwise pass every threshold and prove nothing at all.
        print("coverage: the aggregate report is empty, so nothing was measured", file=sys.stderr)
        return 1

    packages = defaultdict(lambda: {"lm": 0, "lc": 0, "bm": 0, "bc": 0})
    totals = {"lm": 0, "lc": 0, "bm": 0, "bc": 0}

    for row in rows:
        name = row["PACKAGE"]
        for key, column in (("lm", "LINE_MISSED"), ("lc", "LINE_COVERED"),
                            ("bm", "BRANCH_MISSED"), ("bc", "BRANCH_COVERED")):
            value = int(row[column])
            packages[name][key] += value
            totals[key] += value

    def ratio(covered, missed):
        total = covered + missed
        return 100.0 * covered / total if total else 100.0

    line = ratio(totals["lc"], totals["lm"])
    branch = ratio(totals["bc"], totals["bm"])

    print("coverage: line {:.1f}% (min {:.1f}%), branch {:.1f}% (min {:.1f}%)".format(
        line, min_line, branch, min_branch))

    for name in sorted(packages):
        counts = packages[name]
        print("  {:<40} line {:5.1f}%  branch {:5.1f}%".format(
            name, ratio(counts["lc"], counts["lm"]), ratio(counts["bc"], counts["bm"])))

    failures = []
    if line < min_line:
        failures.append("line coverage {:.1f}% is below the required {:.1f}%".format(line, min_line))
    if branch < min_branch:
        failures.append("branch coverage {:.1f}% is below the required {:.1f}%".format(branch, min_branch))

    if failures:
        print("", file=sys.stderr)
        for failure in failures:
            print("coverage: {}".format(failure), file=sys.stderr)
        # Point at the worst offender: a threshold failure with no direction is a
        # threshold people lower rather than meet.
        worst = min(packages, key=lambda n: ratio(packages[n]["lc"], packages[n]["lm"]))
        print("coverage: least covered package is {} at {:.1f}% line".format(
            worst, ratio(packages[worst]["lc"], packages[worst]["lm"])), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
