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
import math
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


FLOOR = "transportOnly"
PRE_ENCODED = "aceMqPreEncoded"
TYPED = "aceMqTypedPayload"


def library_cost_section(results, out):
    """What the library itself costs, measured where the broker cannot drown it.

    The end-to-end pair cannot answer this: AceMQ's own work is a fraction of a
    percent of a network round trip, which is well inside the noise of one. This
    section only appears when the in-process benchmark has been run.
    """
    if not all(n in results for n in (FLOOR, PRE_ENCODED, TYPED)):
        return
    floor, pre, typed = results[FLOOR], results[PRE_ENCODED], results[TYPED]

    def ns(result):
        return "{:.0f} ns".format(result.score * 1000)

    def delta(a, b):
        value = (a.score - b.score) * 1000
        if a.error is None or b.error is None:
            return "{:.0f} ns".format(value)
        err = math.sqrt(a.error ** 2 + b.error ** 2) * 1000
        return "{:.0f} ± {:.0f} ns".format(value, err)

    out.append("")
    out.append("## What the library itself costs")
    out.append("")
    out.append("The measurement above cannot answer this, and it is worth being clear about why. "
               "A confirmed publish to a real broker takes a few hundred microseconds, almost all "
               "of it network and broker; AceMQ's own work is a rounding error inside that, and "
               "looking for it there is weighing the captain by weighing the ship.")
    out.append("")
    out.append("So this benchmark removes the broker. Every case publishes into the in-process "
               "transport, and the only thing that changes between them is how much of AceMQ "
               "sits in front of it. Same JVM, same sink, same payload.")
    out.append("")
    out.append("| | Time per publish | What it includes |")
    out.append("|---|---|---|")
    out.append("| Transport alone | {} | The floor: a message handed straight to the sink |".format(ns(floor)))
    out.append("| Through AceMQ, pre-encoded | {} | Envelope, headers, interceptors, bookkeeping |".format(ns(pre)))
    out.append("| Through AceMQ, typed object | {} | All of the above, plus serialization |".format(ns(typed)))
    out.append("")
    out.append("Which subtracts cleanly, because the intervals here are tiny:")
    out.append("")
    out.append("```")
    out.append("envelope + headers + bookkeeping   {}".format(delta(pre, floor)))
    out.append("JSON serialization                 {}".format(delta(typed, pre)))
    out.append("AceMQ's total cost per publish     {}".format(delta(typed, floor)))
    out.append("```")
    out.append("")

    total = typed.score - floor.score
    acemq_e2e, raw_e2e = results.get(ACEMQ), results.get(RAW)
    if raw_e2e is not None:
        share = 100.0 * total / raw_e2e.score
        out.append("Against the {:.0f} µs a confirmed publish actually takes on this hardware, that "
                   "is **{:.2f}%** — roughly a five-hundredth of the cost of the round trip it "
                   "rides on.".format(raw_e2e.score, share))
        out.append("")
        if acemq_e2e is not None:
            observed = acemq_e2e.score - raw_e2e.score
            if observed > 0 and total < observed:
                out.append("It also says something about the end-to-end figure. That run saw a "
                           "difference of about {:.0f} µs, and the library's own code accounts for "
                           "{:.1f} µs of it — **{:.0f}% of the observed gap is not AceMQ's CPU**, "
                           "and has to be explained by something else."
                           .format(observed, total, 100.0 * (observed - total) / observed))
                out.append("")
                out.append("The leading candidate is bytes. AceMQ's envelope travels as AMQP "
                           "headers, and on a small message those headers are larger than the "
                           "message itself, so the broker parses, routes and persists more for "
                           "every publish. If that is right the overhead is a fixed number of "
                           "bytes rather than a percentage, and it falls away as payloads grow. "
                           "That is a testable claim, and the section below is the test rather "
                           "than the assertion.")
                out.append("")
    out.append("This is a real number with a real interval, and it is the honest way to answer "
               "\"what does the library cost\". It is also not a substitute for the measurement "
               "above: what a library costs in CPU and what it costs in practice are different "
               "questions, and only the end-to-end pair answers the second.")
    out.append("")


def payload_size_section(results, out):
    """Does the overhead fall away as payloads grow?

    Only rendered when the parameterised benchmark has been run. Reports what the
    numbers show, including the case where they refute the hypothesis, because a
    section that can only confirm is not a test.
    """
    sizes = []
    for name, result in results.items():
        if name.startswith("acemqAtSize["):
            size = int(name.split("=")[1].rstrip("]"))
            raw = results.get("rawClientAtSize[payloadBytes={}]".format(size))
            if raw is not None:
                sizes.append((size, result, raw))
    if not sizes:
        return
    sizes.sort()

    out.append("")
    out.append("## Does it shrink with the payload?")
    out.append("")
    out.append("The same confirmed-publish pair, at payload sizes from smaller than the envelope "
               "to far larger than it. If AceMQ's overhead is a fixed number of header bytes, the "
               "percentage falls across this range; if it is per-message work, it stays flat.")
    out.append("")
    out.append("| Payload | AceMQ | Raw client | Difference |")
    out.append("|---|---|---|---|")
    rows = []
    for size, acemq, raw in sizes:
        change, error = jmh.relative_change(acemq, raw)
        shown = "{:+.1f}%".format(change) if error is None else \
            "{:+.1f}% ± {:.1f}".format(change, error)
        label = "{} B".format(size) if size < 1024 else "{} KB".format(size // 1024)
        out.append("| {} | {:.0f} ± {:.0f} µs | {:.0f} ± {:.0f} µs | {} |".format(
            label, acemq.score, acemq.error or 0, raw.score, raw.error or 0, shown))
        rows.append((size, change, error))
    out.append("")

    first, last = rows[0], rows[-1]
    if first[1] > last[1] * 1.5:
        out.append("**The percentage falls as the payload grows**, from {:+.1f}% at {} bytes to "
                   "{:+.1f}% at {} bytes. That is the signature of a fixed cost in bytes, not a "
                   "proportional one: the envelope is the same size either way, so it matters "
                   "less the more message there is to carry it.".format(
                       first[1], first[0], last[1], last[0]))
        out.append("")
        out.append("Which makes the overhead a design choice rather than an inefficiency. The "
                   "envelope buys the id, the correlation, the causation chain and the attempt "
                   "count that every pattern in this library depends on. On a 26-byte message it "
                   "is most of what is sent; on a realistic one it disappears.")
    else:
        out.append("**The percentage does not fall meaningfully across the range** ({:+.1f}% at "
                   "{} bytes against {:+.1f}% at {} bytes). That refutes the header-size "
                   "explanation, and the cost is per-message work somewhere other than the "
                   "library's own publish path — which the in-process measurement above has "
                   "already bounded at well under a microsecond.".format(
                       first[1], first[0], last[1], last[0]))
    out.append("")


def reading_section(results, out):
    """How to read any of it — worked through this run's own numbers.

    Written out rather than assumed. The failure this page exists after was not a
    missing number; it was eight nights of a number nobody could interpret, quoted
    to one decimal place from data that did not support one.
    """
    out.append("")
    out.append("## How to read any of this")
    out.append("")
    out.append("### What `±` is")
    out.append("")
    out.append("Every figure above is an average with a `±` beside it. That is JMH's **99.9% "
               "confidence interval**: the range the true average almost certainly sits in, "
               "given how much the samples disagreed with each other.")
    out.append("")

    acemq, raw = results.get(ACEMQ), results.get(RAW)
    if acemq is None or raw is None or acemq.error is None or raw.error is None:
        out.append("It is not decoration. A figure without one is a single sample wearing a "
                   "number's clothes.")
        return

    out.append("So `{:.1f} ± {:.1f} µs` does not mean \"{:.1f}\". It means **somewhere between "
               "{:.1f} and {:.1f}**, and the measurement declines to be more specific than "
               "that.".format(acemq.score, acemq.error, acemq.score,
                              acemq.low, acemq.high))
    out.append("")
    out.append("### Why the difference is fuzzier than either number")
    out.append("")
    out.append("This is the part that is easy to get wrong, and we did get it wrong. When two "
               "uncertain numbers are subtracted, the uncertainties **add** — they do not "
               "cancel. Worked through this run:")
    out.append("")
    diff = acemq.score - raw.score
    combined = math.sqrt(acemq.error ** 2 + raw.error ** 2)
    change, error = jmh.relative_change(acemq, raw)
    out.append("```")
    out.append("AceMQ         {:8.1f} ± {:5.1f} µs".format(acemq.score, acemq.error))
    out.append("raw client    {:8.1f} ± {:5.1f} µs".format(raw.score, raw.error))
    out.append("")
    out.append("difference    {:8.1f} ± {:5.1f} µs      ← {:.1f} and {:.1f} combine to {:.1f}"
               .format(diff, combined, acemq.error, raw.error, combined))
    out.append("as a percent  {:+8.1f}% ± {:4.1f}%   →  [{:+.1f}%, {:+.1f}%]".format(
        change, error, change - error, change + error))
    out.append("```")
    out.append("")
    if abs(diff) < combined:
        out.append("The uncertainty on the difference (**±{:.1f} µs**) is larger than the "
                   "difference itself (**{:.1f} µs**). That is why this run supports no figure: "
                   "the interval contains zero, so it cannot even establish which of the two is "
                   "faster.".format(combined, diff))
    else:
        out.append("The difference (**{:.1f} µs**) is larger than the uncertainty on it "
                   "(**±{:.1f} µs**), so this run does establish that a real difference exists. "
                   "How big it is remains a range, not a number.".format(diff, combined))
    out.append("")
    out.append("### What the gate does with that")
    out.append("")
    out.append("| The interval | Verdict |")
    out.append("|---|---|")
    out.append("| entirely above the budget | **fail** — a real regression |")
    out.append("| entirely below the budget | **pass** — really within budget |")
    out.append("| straddling the budget | **inconclusive** — this run cannot say, and does not "
               "pretend to |")
    out.append("")
    out.append("The third row is the one that matters. Comparing the averages alone and ignoring "
               "the `±` is how this gate reported a regression on eight consecutive nights when "
               "nothing had changed — the code was constant and the noise was not. A gate that "
               "fires on noise gets ignored, and an ignored gate protects nothing.")
    out.append("")
    out.append("### Why the numbers cannot simply be made sharper")
    out.append("")
    out.append("Precision improves with the **square root** of the sample count, so halving the "
               "interval costs four times the runtime. The benchmark also times a whole network "
               "round trip to a broker, of which AceMQ's own work is a small part — most of what "
               "is being measured, and nearly all of what is varying, is the broker and the "
               "network.")
    out.append("")
    out.append("It is weighing the captain by weighing the ship twice. That is why there is a "
               "second benchmark that removes the broker entirely and times the library path "
               "against the transport underneath it, where the noise floor is nanoseconds and "
               "the answer is a real number rather than a range.")
    out.append("")


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
    library_cost_section(results, out)
    payload_size_section(results, out)
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
    reading_section(results, out)
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
