#!/usr/bin/env python3
"""Enforce the overhead budget: AceMQ against the client it wraps.

Doc 10 allows AceMQ to cost at most a few percent more than a hand-written
publish with the same guarantees. Both figures come from the same JMH run on the
same machine against the same broker, so the ratio between them survives the
noise that makes an absolute number from shared hardware meaningless.

What it does not survive is being read off the means. On a shared runner the two
figures arrive as 252 +/- 17 and 236 +/- 15 us/op: the difference is 16 us/op
with a combined uncertainty of 22, which is a measurement that cannot tell 0%
from 13% and was being reported as "+6.8% FAIL" every night.

So the budget is enforced against the interval. The run fails only when the whole
confidence interval sits above the budget -- when even the most favourable
reading of the numbers is a breach. A run whose interval straddles the budget is
reported as inconclusive and passes, with the precision it would need printed
next to it, because a gate that fires on noise is a gate people learn to ignore.

Usage:
    check-overhead-budget.py <results.json> [max-overhead-percent]

Exit codes:
    0  within budget, or too imprecise to say
    1  over budget, and the interval agrees
    2  the run cannot be judged: a benchmark is missing, or has no error bars
"""

import sys

import jmh

ACEMQ = "acemqConfirmedPublish"
RAW = "rawClientConfirmedPublish"


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    limit = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
    results = jmh.load(sys.argv[1])

    if ACEMQ not in results or RAW not in results:
        # Refusing to pass is the point: a missing pair means the comparison did
        # not happen, and reporting success for a measurement nobody made is how
        # a budget silently stops being enforced.
        print("overhead: both {} and {} must be present; found {}".format(
            ACEMQ, RAW, ", ".join(sorted(results)) or "nothing"), file=sys.stderr)
        return 2

    acemq, raw = results[ACEMQ], results[RAW]
    change, error = jmh.relative_change(acemq, raw)

    print("overhead: measured on {}".format(jmh.environment(results)))
    print("  {:<28} {}".format(ACEMQ, acemq.format()))
    print("  {:<28} {}".format(RAW, raw.format()))

    if error is None:
        # JMH reports no interval below three data points. That is a benchmark
        # configuration problem, not a result, and passing on it would mean the
        # budget is enforced against a single sample.
        print("")
        print("overhead: {:+.1f}% on the means, and no confidence interval to judge it by."
              .format(change), file=sys.stderr)
        print("overhead: JMH reports no error for fewer than three data points; raise "
              "@Fork or @Measurement.", file=sys.stderr)
        return 2

    print("  {:<28} {:+.1f}% [{:+.1f}%, {:+.1f}%] against a budget of {:.1f}%".format(
        "overhead", change, change - error, change + error, limit))

    outcome = jmh.verdict(change, error, limit)
    if outcome == "over":
        print("")
        print("overhead: {:+.1f}% exceeds the {:.1f}% budget, and the whole interval "
              "[{:+.1f}%, {:+.1f}%] is above it.".format(
                  change, limit, change - error, change + error), file=sys.stderr)
        return 1

    if outcome == "inconclusive":
        # Not a failure, and not a pass either. Saying so is the honest report,
        # and the number that would change it is printed rather than implied.
        print("")
        print("overhead: inconclusive. The interval [{:+.1f}%, {:+.1f}%] straddles the "
              "{:.1f}% budget.".format(change - error, change + error, limit))
        print("overhead: this run resolves +/-{:.1f}%; deciding a {:.1f}% budget needs the "
              "interval inside it.".format(error, limit))
        print("overhead: not failing the build on a measurement that cannot answer the "
              "question.")
        return 0

    print("")
    print("overhead: within budget. The whole interval [{:+.1f}%, {:+.1f}%] is under "
          "{:.1f}%.".format(change - error, change + error, limit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
