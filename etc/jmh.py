#!/usr/bin/env python3
"""Reading JMH results, and deciding what they support.

Shared by check-overhead-budget.py and compare-benchmarks.py, because both were
answering the same question badly: they compared two means and ignored the error
bars printed next to them.

JMH's ``scoreError`` is the half-width of the 99.9% confidence interval of the
mean. A verdict that ignores it is a verdict about noise. On a shared GitHub
runner a network round-trip measures 250 +/- 17 us/op, which is +/-7%, so a 5%
budget enforced on the means alone reports a regression roughly half the nights
whatever the code does -- which is what happened here every night from 27 August.

The rule everything below applies: a difference counts only when the whole
confidence interval is on one side of the threshold. Anything else is reported,
in the words that fit it, and does not fail a build.
"""

import json
import math


class Result:
    """One benchmark, and the confidence interval JMH measured for it."""

    def __init__(self, name, record):
        metric = record["primaryMetric"]
        self.name = name
        self.score = metric["score"]
        error = metric.get("scoreError")
        # JMH writes NaN when it has too few data points to compute an interval,
        # which json.load happily turns into float('nan'). Carrying it as None
        # makes every caller decide what to do about it rather than propagating a
        # NaN through arithmetic that then compares false against everything.
        self.error = None if error is None or error != error else error
        self.unit = metric["scoreUnit"]
        self.forks = record.get("forks")
        self.iterations = record.get("measurementIterations")
        self.jdk = record.get("jdkVersion")
        self.vm = record.get("vmName")

    @property
    def low(self):
        return self.score - (self.error or 0.0)

    @property
    def high(self):
        return self.score + (self.error or 0.0)

    def format(self):
        if self.error is None:
            return "{:8.1f} (no interval) {}".format(self.score, self.unit)
        return "{:8.1f} +/- {:5.1f} {}".format(self.score, self.error, self.unit)


def load(path):
    """Reads a JMH JSON file into {short benchmark name: Result}.

    A parameterised benchmark appears once per parameter combination under the same
    name, so the key carries the parameters when there are any -- keying on the name
    alone would silently keep whichever entry happened to be last, and a check that
    quietly drops half its inputs is the failure this module exists after.
    """
    with open(path) as handle:
        records = json.load(handle)
    loaded = {}
    for record in records:
        name = record["benchmark"].split(".")[-1]
        params = record.get("params") or {}
        if params:
            name += "[" + ",".join("{}={}".format(k, params[k]) for k in sorted(params)) + "]"
        loaded[name] = Result(name, record)
    return loaded


def relative_change(current, reference):
    """Percentage change from reference to current, with its own interval.

    Lower is better in average-time mode, so a positive number is slower.

    The uncertainty is standard propagation through ``(a - b) / b``, at the
    confidence level JMH used for its own intervals. Both errors are needed: a
    tight current run compared against a baseline measured once on a laptop is
    exactly as uncertain as that baseline was.

    Returns ``(change, error)``, where error is None when either side has no
    interval -- in which case no verdict is available, rather than a verdict of
    zero uncertainty.
    """
    change = 100.0 * (current.score - reference.score) / reference.score
    if current.error is None or reference.error is None:
        return change, None
    error = 100.0 * math.sqrt(
        (current.error / reference.score) ** 2
        + (current.score * reference.error / reference.score ** 2) ** 2
    )
    return change, error


def verdict(change, error, limit):
    """Classifies a change against a threshold.

    ``over``          the whole interval is above the limit: a real regression
    ``within``        the whole interval is below the limit: a real pass
    ``inconclusive``  the interval straddles the limit: this run cannot say
    ``unmeasured``    no interval at all
    """
    if error is None:
        return "unmeasured"
    if change - error > limit:
        return "over"
    if change + error <= limit:
        return "within"
    return "inconclusive"


def environment(results):
    """A fingerprint of what produced these numbers.

    Two runs are comparable when this matches. Comparing across it is how a
    baseline measured on a laptop on 26 August came to be enforced against
    ubuntu-latest every night since, and the difference it reported was the
    hardware.
    """
    jdks = {r.jdk for r in results.values() if r.jdk}
    vms = {r.vm for r in results.values() if r.vm}
    return "jdk={} vm={}".format(
        ",".join(sorted(jdks)) or "unknown", ",".join(sorted(vms)) or "unknown")
