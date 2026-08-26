#!/usr/bin/env python3
"""Enforce the overhead budget: AceMQ against the client it wraps.

Doc 10 allows AceMQ to cost at most a few percent more than a hand-written
publish with the same guarantees. Both figures come from the same JMH run on the
same machine against the same broker, so the ratio between them survives the
noise that makes an absolute number from shared hardware meaningless.

Usage:
    check-overhead-budget.py <results.json> [max-overhead-percent]
"""

import json
import sys

ACEMQ = "acemqConfirmedPublish"
RAW = "rawClientConfirmedPublish"


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    limit = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0

    with open(sys.argv[1]) as handle:
        results = {r["benchmark"].split(".")[-1]: r["primaryMetric"] for r in json.load(handle)}

    if ACEMQ not in results or RAW not in results:
        # Refusing to pass is the point: a missing pair means the comparison did
        # not happen, and reporting success for a measurement nobody made is how
        # a budget silently stops being enforced.
        print("overhead: both {} and {} must be present; found {}".format(
            ACEMQ, RAW, ", ".join(sorted(results)) or "nothing"), file=sys.stderr)
        return 1

    acemq = results[ACEMQ]["score"]
    raw = results[RAW]["score"]
    overhead = 100.0 * (acemq - raw) / raw

    print("overhead: AceMQ {:.1f} us/op, raw client {:.1f} us/op".format(acemq, raw))
    print("overhead: {:+.1f}% against a budget of {:.1f}%".format(overhead, limit))

    # Error bars are reported so a tight pass on wide intervals is visible rather
    # than mistaken for precision.
    for name, metric in ((ACEMQ, results[ACEMQ]), (RAW, results[RAW])):
        error = metric.get("scoreError")
        if error is not None and error == error:  # not NaN
            print("  {:<28} {:8.1f} +/- {:.1f} us/op".format(name, metric["score"], error))

    if overhead > limit:
        print("", file=sys.stderr)
        print("overhead: {:.1f}% exceeds the {:.1f}% budget".format(overhead, limit), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
