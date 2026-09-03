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

    def check(self, acemq, acemq_error, raw, raw_error, budget="5"):
        path = results_file([
            record("acemqConfirmedPublish", acemq, acemq_error),
            record("rawClientConfirmedPublish", raw, raw_error),
        ])
        try:
            return run("check-overhead-budget.py", path, budget)
        finally:
            os.unlink(path)

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
        self.assertIn("exceeds the 5.0% budget", output)

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
            code, output = run("check-overhead-budget.py", path, "5")
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
