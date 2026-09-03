#!/usr/bin/env python3
"""Render docs/benchmarks.md from a JMH result file.

    python3 etc/render-benchmarks-page.py <results.json> [--run-url URL] [--out docs/benchmarks.md]

Generated, never hand-written, and for the same reason the version strings are:
a number typed into a document is correct on the day it is typed. Everything on
the page below comes out of the JSON -- scores, intervals, fork and iteration
counts, JDK, VM -- so the page cannot drift from the run that produced it, and a
run that did not measure something produces a page that does not claim it.

Every figure is printed with its confidence interval, and the parity claim is
expressed as an interval rather than a number. That is not modesty. AceMQ wraps
the RabbitMQ client, so it cannot be faster than the client at the same work, and
the only defensible claim is how much it costs -- which on shared hardware is
frequently "less than this measurement can resolve".
"""

import argparse
import datetime
import sys

import jmh

ACEMQ = "acemqConfirmedPublish"
RAW = "rawClientConfirmedPublish"

TELEMETRY = [
    ("publishWithoutTelemetry", "No telemetry"),
    ("publishWithMetrics", "Micrometer metrics"),
    ("publishWithTracing", "OpenTelemetry tracing"),
    ("publishWithMetricsAndTracing", "Both"),
]


def interval(result):
    if result.error is None:
        return "{:.2f} (no interval)".format(result.score)
    return "{:.2f} ± {:.2f}".format(result.score, result.error)


def parity_section(results, out):
    if ACEMQ not in results or RAW not in results:
        out.append("This run did not measure the pair, so this page makes no claim about it.\n")
        return

    acemq, raw = results[ACEMQ], results[RAW]
    change, error = jmh.relative_change(acemq, raw)

    out.append("| | Time per confirmed publish | |")
    out.append("|---|---|---|")
    out.append("| **AceMQ** | {} µs | `{}` |".format(interval(acemq), ACEMQ))
    out.append("| **RabbitMQ client, by hand** | {} µs | `{}` |".format(interval(raw), RAW))
    out.append("")

    if error is None:
        out.append("This run produced no confidence interval, so it supports no comparison. "
                   "That is a benchmark configuration problem rather than a result.")
        return

    low, high = change - error, change + error
    if error > 100.0:
        out.append("**This run does not constrain the difference at all**: its interval is wider "
                   "than the figure it surrounds. Nothing is quoted from it, and the gate treats "
                   "it the same way.")
        return
    out.append("The difference is **{:+.1f}%, somewhere in [{:+.1f}%, {:+.1f}%]** at JMH's 99.9% "
               "confidence.".format(change, low, high))
    out.append("")
    if low <= 0 <= high:
        out.append("That interval contains zero. On this hardware, a publish through AceMQ is "
                   "**indistinguishable from the same publish written by hand** — not equal to "
                   "it, indistinguishable from it, which is the strongest thing a measurement "
                   "of this precision can support.")
    elif high < 0:
        out.append("The interval is entirely below zero, which would make AceMQ faster than the "
                   "client it wraps. Treat that as a measurement artefact rather than a result: "
                   "the same work cannot cost less through more layers.")
    else:
        out.append("The interval is entirely above zero, so this run does show a cost: at least "
                   "{:+.1f}% and at most {:+.1f}%.".format(low, high))
    out.append("")
    out.append("The nightly gate fails when that whole interval sits above five percent. It "
               "compares the interval rather than the two averages, because on shared hardware "
               "the averages alone report a breach whichever way the noise falls — which they "
               "did, every night, for eight nights in August.")


def telemetry_section(results, out):
    present = [(name, label) for name, label in TELEMETRY if name in results]
    if not present:
        return
    out.append("")
    out.append("## What instrumentation costs")
    out.append("")
    out.append("Measured over the in-memory transport, deliberately. A real broker's round trip "
               "is measured in hundreds of microseconds and would swamp a difference measured in "
               "single ones, turning this into a benchmark of RabbitMQ.")
    out.append("")
    out.append("| Configuration | Time per publish |")
    out.append("|---|---|")
    for name, label in present:
        out.append("| {} | {} µs |".format(label, interval(results[name])))
    out.append("")
    base = results.get("publishWithoutTelemetry")
    both = results.get("publishWithMetricsAndTracing")
    if base and both:
        change, error = jmh.relative_change(both, base)
        if error is None:
            out.append("No interval on this run, so no claim about the difference.")
        elif error > 100.0:
            # An interval wider than the quantity is not a loose measurement, it is
            # an absence of one, and printing "-58.9%, interval [-255%, +137%]"
            # dresses that up as a finding. Say what happened instead.
            out.append("**This run does not constrain the difference at all**: its interval is "
                       "wider than the figure it surrounds, which happens when a microsecond-"
                       "scale benchmark shares a machine with something else. No number is "
                       "quoted from it.")
        elif change - error <= 0 <= change + error:
            out.append("Metrics and tracing together are **not distinguishable from no telemetry "
                       "at this precision** ({:+.1f}%, interval [{:+.1f}%, {:+.1f}%]). Publish a "
                       "tighter number only when a run resolves one.".format(
                           change, change - error, change + error))
        else:
            out.append("Metrics and tracing together cost **{:+.1f}%, interval [{:+.1f}%, "
                       "{:+.1f}%]**.".format(change, change - error, change + error))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results")
    parser.add_argument("--run-url", default=None)
    parser.add_argument("--out", default="docs/benchmarks.md")
    args = parser.parse_args()

    try:
        results = jmh.load(args.results)
    except FileNotFoundError:
        # No run to render. The page still exists, because the nav links to it,
        # and it says the only true thing available: nobody has measured this
        # yet. A page of placeholder numbers would be worse than no page.
        with open(args.out, "w") as handle:
            handle.write("# Benchmarks\n\n"
                         "<!-- Generated by etc/render-benchmarks-page.py. Do not edit. -->\n\n"
                         "No benchmark run has been recorded yet, so this page has no numbers "
                         "on it.\n\nThe nightly `nightly-benchmarks` workflow produces them, and "
                         "this page is regenerated from its most recent successful run. Nothing "
                         "here is written by hand, so until that run exists there is nothing to "
                         "show.\n")
        print("wrote {} with no data: {} does not exist".format(args.out, args.results))
        return 0
    if not results:
        print("no benchmarks in {}; refusing to write a page with nothing on it".format(
            args.results), file=sys.stderr)
        return 1

    sample = next(iter(results.values()))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out = ["# Benchmarks", ""]
    out.append("<!-- Generated by etc/render-benchmarks-page.py. Do not edit: the next run "
               "overwrites it. -->")
    out.append("")
    out.append("Every number here comes out of a JMH run, with the confidence interval that run "
               "measured. Nothing on this page is typed by hand, because a number typed into a "
               "document is correct on the day it is typed.")
    out.append("")
    out.append("## Parity with the client it wraps")
    out.append("")
    out.append("The one measurement that matters, and the one behind the claim in the README: a "
               "confirmed publish through AceMQ against the same publish written by hand with "
               "the RabbitMQ client — confirm mode, persistent delivery, mandatory, waiting for "
               "the broker. Both cases run in the same JVM, against the same broker, minutes "
               "apart, so most of the machine's noise cancels between them.")
    out.append("")
    parity_section(results, out)
    telemetry_section(results, out)

    out.append("")
    out.append("## What this page does not claim")
    out.append("")
    out.append("**No comparison against other libraries.** AceMQ wraps the RabbitMQ client. At "
               "the same work it cannot beat the thing it calls, and a benchmark showing "
               "otherwise would be measuring different work.")
    out.append("")
    out.append("**No capacity figure for your broker.** These runs share a CPU with the broker "
               "in a container on a hosted runner. That measures the runner. For a number about "
               "your own broker, use "
               "[acemq-java-amqp-workloads](https://github.com/AceMQ-Company/acemq-java-amqp-workloads), "
               "which exists for exactly that and reports what it measured rather than what you "
               "should conclude.")
    out.append("")
    out.append("**No figure for where the patterns win.** A retry ladder does beat a "
               "`Thread.sleep` in a handler, and the reason is arithmetic rather than "
               "micro-optimisation: a sleeping handler holds its channel, so everything "
               "prefetched behind it waits with it, while the ladder republishes and frees the "
               "consumer. That is a mechanism, not a measurement, and no benchmark here has "
               "measured it yet — so no multiplier is quoted.")
    out.append("")
    out.append("## How to read the intervals")
    out.append("")
    out.append("JMH's `±` is the half-width of the 99.9% confidence interval of the mean. Two "
               "figures whose intervals overlap have not been shown to differ. A gate that "
               "ignores the interval and compares the averages is a gate that fires on noise, "
               "and both checks in `etc/` refuse to reach a verdict unless a whole interval is "
               "on one side of the threshold.")
    out.append("")
    out.append("## How this run was measured")
    out.append("")
    out.append("| | |")
    out.append("|---|---|")
    out.append("| Forks × measurement iterations | {} × {} |".format(
        sample.forks, sample.iterations))
    out.append("| JDK | {} |".format(sample.jdk or "unknown"))
    out.append("| VM | {} |".format(sample.vm or "unknown"))
    out.append("| Benchmarks | {} |".format(len(results)))
    if args.run_url:
        out.append("| Run | [{}]({}) |".format(args.run_url.rsplit("/", 1)[-1], args.run_url))
    out.append("| Page generated | {} |".format(stamp))
    out.append("")
    out.append("Reproduce it:")
    out.append("")
    out.append("```bash")
    out.append("mvn -Pbenchmarks -DskipTests install")
    out.append("java -jar acemq-amqp-benchmarks/target/benchmarks.jar \\")
    out.append("     -rf json -rff benchmarks/results/current.json")
    out.append("python3 etc/check-overhead-budget.py benchmarks/results/current.json 5")
    out.append("```")
    out.append("")
    out.append("Docker is required: the publish benchmark starts its own broker.")
    out.append("")

    with open(args.out, "w") as handle:
        handle.write("\n".join(out))
    print("wrote {} from {} benchmark(s)".format(args.out, len(results)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
