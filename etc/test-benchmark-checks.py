#!/usr/bin/env python3
"""Tests for the benchmark gate.

    python3 etc/test-benchmark-checks.py

Standard library only, so it runs anywhere the checks themselves run.

These exist because the gate was wrong for eight nights and nothing said so: a
check with no test is a check that can only be verified by the thing it is
supposed to protect. The fixtures are real numbers from the runs that exposed it.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

OVER_BUDGET = 1
UNJUDGEABLE = 2


def record(name, score, error, jdk="21.0.2", vm="OpenJDK 64-Bit Server VM"):
    return {
        "benchmark": "org.acemq.amqp.benchmarks.PublishOverheadBenchmark." + name,
        "mode": "avgt",
        "forks": 3,
        "measurementIterations": 10,
        "jdkVersion": jdk,
        "vmName": vm,
        "primaryMetric": {
            "score": score,
            "scoreError": error,
            "scoreUnit": "us/op",
        },
    }


def results_file(records):
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(records, handle)
    handle.close()
    return handle.name


def run(script, *args):
    completed = subprocess.run(
        [sys.executable, os.path.join(HERE, script)] + list(args),
        cwd=ROOT, capture_output=True, text=True)
    return completed.returncode, completed.stdout + completed.stderr


class OverheadBudget(unittest.TestCase):

    def check(self, acemq, acemq_error, raw, raw_error, budget="10"):
        path = results_file([
            record("acemqConfirmedPublish", acemq, acemq_error),
            record("rawClientConfirmedPublish", raw, raw_error),
        ])
        try:
            args = [path] if budget is None else [path, budget]
            return run("check-overhead-budget.py", *args)
        finally:
            os.unlink(path)

    def test_the_budget_defaults_to_ten_percent(self):
        # The workflow passes the limit explicitly, so nothing else would notice
        # if the default drifted away from the number the project agreed on.
        code, output = self.check(240.0, 4.0, 236.0, 4.0, budget=None)
        self.assertEqual(0, code, output)
        self.assertIn("budget of 10.0%", output)

    def test_the_night_that_started_this_is_inconclusive_not_a_failure(self):
        # 3 September: 252.2 +/- 16.7 against 236.2 +/- 14.8, reported as
        # "+6.8% FAIL". The difference is 16 us/op with a combined uncertainty of
        # 22, which cannot tell 0% from 13%.
        code, output = self.check(252.2, 16.7, 236.2, 14.8)
        self.assertEqual(0, code, output)
        self.assertIn("inconclusive", output)

    def test_a_real_regression_still_fails(self):
        # Tight intervals, and a gap far outside them.
        code, output = self.check(300.0, 4.0, 236.0, 4.0)
        self.assertEqual(OVER_BUDGET, code, output)
        self.assertIn("exceeds the 10.0% budget", output)

    def test_over_the_ten_percent_budget_fails(self):
        # +14.4%, interval [+11.8%, +17.0%]: entirely above 10%, so the gate has
        # to fail at its stated number and not merely somewhere above it.
        code, output = self.check(270.0, 4.0, 236.0, 4.0)
        self.assertEqual(OVER_BUDGET, code, output)
        self.assertIn("exceeds the 10.0% budget", output)
        self.assertIn("is above it", output)

    def test_between_five_and_ten_percent_passes_now_and_would_not_have(self):
        # +7.5%, interval [+6.3%, +8.7%] -- entirely above the old 5% budget and
        # entirely below the new one. This is the band the change actually moved,
        # so it is asserted in both directions: a pass at 10%, a failure at 5%.
        code, output = self.check(253.7, 2.0, 236.0, 2.0)
        self.assertEqual(0, code, output)
        self.assertIn("within budget", output)
        self.assertIn("under 10.0%", output)

        code, output = self.check(253.7, 2.0, 236.0, 2.0, budget="5")
        self.assertEqual(OVER_BUDGET, code, output)
        self.assertIn("exceeds the 5.0% budget", output)

    def test_the_measured_overhead_is_inside_the_new_budget(self):
        # The 3x10 run behind the decision: 452.7 +/- 13.2 against 433.8 +/- 11.6,
        # which is +4.4% with an interval of [+0.2%, +8.5%]. It excludes zero --
        # the library is measurably slower -- and it sits inside 10%, whereas at
        # 5% the same numbers could only ever be reported as inconclusive.
        code, output = self.check(452.7, 13.2, 433.8, 11.6)
        self.assertEqual(0, code, output)
        self.assertIn("within budget", output)

        code, output = self.check(452.7, 13.2, 433.8, 11.6, budget="5")
        self.assertEqual(0, code, output)
        self.assertIn("inconclusive", output)

    def test_a_clean_run_passes(self):
        code, output = self.check(240.0, 4.0, 236.0, 4.0)
        self.assertEqual(0, code, output)
        self.assertIn("within budget", output)

    def test_faster_than_the_raw_client_passes(self):
        code, output = self.check(230.0, 4.0, 236.0, 4.0)
        self.assertEqual(0, code, output)

    def test_no_error_bars_cannot_be_judged(self):
        # JMH writes NaN below three data points. Passing on that would enforce
        # the budget against a single sample.
        code, output = self.check(300.0, float("nan"), 236.0, float("nan"))
        self.assertEqual(UNJUDGEABLE, code, output)
        self.assertIn("no confidence interval", output)

    def test_a_missing_benchmark_cannot_be_judged(self):
        path = results_file([record("acemqConfirmedPublish", 252.0, 16.0)])
        try:
            code, output = run("check-overhead-budget.py", path, "10")
        finally:
            os.unlink(path)
        self.assertEqual(UNJUDGEABLE, code, output)
        self.assertIn("must be present", output)


class BaselineComparison(unittest.TestCase):

    def compare(self, current, baseline, limit="15"):
        current_path, baseline_path = results_file(current), results_file(baseline)
        try:
            return run("compare-benchmarks.py", current_path, baseline_path, limit)
        finally:
            os.unlink(current_path)
            os.unlink(baseline_path)

    def test_refuses_to_compare_across_environments(self):
        # The fault that made this gate red every night: a baseline measured on a
        # laptop, enforced against ubuntu-latest.
        code, output = self.compare(
            [record("acemqConfirmedPublish", 252.0, 16.0)],
            [record("acemqConfirmedPublish", 190.0, 10.0, jdk="21.0.8",
                    vm="OpenJDK 64-Bit Server VM (laptop)")])
        self.assertEqual(UNJUDGEABLE, code, output)
        self.assertIn("refusing to compare across environments", output)

    def test_noise_inside_the_intervals_is_not_a_regression(self):
        code, output = self.compare(
            [record("acemqConfirmedPublish", 265.0, 20.0)],
            [record("acemqConfirmedPublish", 236.0, 18.0)])
        self.assertEqual(0, code, output)
        self.assertIn("inconclusive", output)

    def test_a_real_regression_fails(self):
        code, output = self.compare(
            [record("acemqConfirmedPublish", 300.0, 4.0)],
            [record("acemqConfirmedPublish", 236.0, 4.0)])
        self.assertEqual(1, code, output)
        self.assertIn("regressed", output)

    def test_a_missing_benchmark_fails(self):
        code, output = self.compare(
            [record("acemqConfirmedPublish", 252.0, 16.0)],
            [record("acemqConfirmedPublish", 252.0, 16.0),
             record("rawClientConfirmedPublish", 236.0, 15.0)])
        self.assertEqual(1, code, output)
        self.assertIn("absent now", output)

    def test_a_missing_baseline_is_reported_and_passes(self):
        path = results_file([record("acemqConfirmedPublish", 252.0, 16.0)])
        try:
            code, output = run("compare-benchmarks.py", path,
                               "benchmarks/results/baseline-does-not-exist.json", "15")
        finally:
            os.unlink(path)
        self.assertEqual(0, code, output)
        self.assertIn("no baseline", output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
