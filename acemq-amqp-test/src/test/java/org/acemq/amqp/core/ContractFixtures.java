/*
 * Copyright 2026 AceMQ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acemq.amqp.core;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.DeliveryListener;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;

/**
 * Writes the retry, naming and topology contract to JSON by asking the real library rather than
 * by writing down what it is believed to do.
 *
 * <p>The reason this exists is a bug that survived ten releases. Java's
 * {@code RetryPolicy.exponential} multiplied by five and jittered by ten percent while Go, .NET,
 * Python and Ruby doubled and jittered by twenty, so {@code exponential(5, 1s, 1m)} produced
 * {@code 1s, 5s, 25s, 60s} in one language and {@code 1s, 2s, 4s, 8s} in the other four. Nothing
 * caught it, because each library tested its own arithmetic against its own expectations and
 * nothing ever compared them. Three further divergences turned up the same week, all of them
 * found by a person reading five codebases side by side.
 *
 * <p>So every number below is computed, never typed. A fixture transcribed from a comment agrees
 * with the comment; a fixture computed from {@link RetryPolicy#schedule()} disagrees with the
 * committed file the moment the schedule changes, which is the only useful behaviour.
 *
 * <p>This class sits in {@code org.acemq.amqp.core} rather than {@code org.acemq.amqp.test}
 * because {@link RetryTopology} — the class that names the rungs and builds their argument table
 * — is package-private there. Reaching it is the point: the alternative is a second copy of the
 * naming rules in the test tree, which is the very thing the fixtures exist to prevent.
 */
public final class ContractFixtures {

    /** The queue every named example hangs off, chosen to have a dot in it. */
    private static final String QUEUE = "orders.new";

    /**
     * Delays whose rendered names are worth pinning, awkward ones included.
     *
     * <p>Sub-second because Java renders it {@code 500ms} where three of the ports render
     * {@code 0s}; exactly a second, exactly the default threshold, a minute and a half that is
     * not a whole number of minutes, and two boundaries that are.
     */
    private static final List<Duration> NAMED_DELAYS = Collections.unmodifiableList(Arrays.asList(
            Duration.ofMillis(500),
            Duration.ofSeconds(1),
            Duration.ofSeconds(30),
            Duration.ofSeconds(90),
            Duration.ofMinutes(5),
            Duration.ofHours(2)));

    /** Low enough that every delay above becomes a rung, so its name can be read off. */
    private static final Duration EVERYTHING_IN_THE_BROKER = Duration.ofMillis(1);

    /**
     * The age a message has to reach before "no age limit" means anything.
     *
     * <p>A year, because a year used to be the Java default: {@code exponential}, {@code fixed}
     * and {@code none} set {@code Duration.ofDays(365)} and compared against it unconditionally,
     * so a message exactly this old was dead-lettered here and retried by Go, .NET, Python and
     * Ruby. The number is in the fixture so that the disagreement cannot come back quietly.
     */
    private static final Duration A_YEAR = Duration.ofDays(365);

    private ContractFixtures() {
        throw new AssertionError("ContractFixtures is a generator and must not be instantiated");
    }

    /**
     * Builds the whole fixture.
     *
     * @return the contents of {@code contract-fixtures.json}
     */
    public static String generate() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("generatedBy", "acemq-java-amqp ContractFixtures");
        root.put(
                "contract",
                "the retry schedule, the queue names and the topology every AceMQ library must agree on");
        root.put("defaultBrokerWaitThresholdMillis", RetryPolicy.DEFAULT_BROKER_WAIT_THRESHOLD.toMillis());
        root.put("deadLetterExchange", Topology.DEAD_LETTER_EXCHANGE);
        root.put("retryExchange", RetryTopology.RETRY_EXCHANGE);
        root.put("retrySchedules", retrySchedules());
        root.put("maxMessageAge", maxMessageAge());
        root.put("jitter", jitter());
        root.put("brokerWaitThreshold", brokerWaitThreshold());
        root.put("naming", naming());
        root.put("rungArguments", rungArguments());
        root.put("topology", topology());
        root.put("queueTypeDefaults", queueTypeDefaults());
        return Json.render(root);
    }

    // ---------------------------------------------------------------- retry schedules

    /**
     * The unjittered delay sequence of a set of named policies.
     *
     * <p>This is the section that would have caught the multiplier. A port reads
     * {@code scheduleMillis} and compares it with what its own {@code exponential} produces; five
     * seconds where the fixture says two is a failing test on the commit that wrote it.
     */
    private static List<Object> retrySchedules() {
        List<Object> schedules = new ArrayList<>();
        schedules.add(schedule(
                "exponential-uncapped",
                "exponential(5, 1s, 24h)",
                RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofHours(24))));
        schedules.add(schedule(
                "exponential-capped",
                "exponential(6, 10s, 1m)",
                RetryPolicy.exponential(6, Duration.ofSeconds(10), Duration.ofMinutes(1))));
        schedules.add(schedule(
                "exponential-give-up-on-age",
                "exponential(5, 1s, 24h).giveUpAfter(2m)",
                RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofHours(24))
                        .giveUpAfter(Duration.ofMinutes(2))));
        schedules.add(schedule("fixed", "fixed(4, 30s)", RetryPolicy.fixed(4, Duration.ofSeconds(30))));
        schedules.add(schedule("none", "none()", RetryPolicy.none()));
        return schedules;
    }

    private static Map<String, Object> schedule(String name, String how, RetryPolicy policy) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("name", name);
        entry.put("how", how);
        entry.put("maxAttempts", (long) policy.maxAttempts());
        entry.put("maxMessageAgeMillis", policy.maxMessageAge().toMillis());
        // Said twice on purpose. A reader who does not know the convention would take a zero for
        // "abandon everything", which is the opposite of what it means, and a port that guessed
        // wrong would dead-letter every message on its first failure.
        entry.put("hasMaxMessageAge", policy.maxMessageAge().toMillis() > 0);
        entry.put("jitterFactor", policy.jitterFactor());
        entry.put("brokerWaitThresholdMillis", policy.brokerWaitThreshold().toMillis());
        entry.put("scheduleMillis", millis(policy.schedule()));
        entry.put("brokerRungDelaysMillis", millis(policy.brokerRungs()));
        entry.put("decisions", decisions(policy));
        return entry;
    }

    /**
     * Whether the policy retries at all, at the attempt and age boundaries.
     *
     * <p>Booleans rather than delays on purpose. A consumer wait has already had jitter applied
     * to it by the time {@code nextWait} answers, and a random number in a fixture is a fixture
     * that means nothing. The delays are in {@code scheduleMillis}, unjittered, where they can
     * be compared.
     */
    private static List<Object> decisions(RetryPolicy policy) {
        List<Object> decisions = new ArrayList<>();
        // A set, because a policy that never retries has one attempt and the first, the
        // penultimate and the last are all the same number.
        java.util.Set<Integer> attempts = new java.util.LinkedHashSet<>();
        attempts.add(1);
        attempts.add(Math.max(1, policy.maxAttempts() - 1));
        attempts.add(policy.maxAttempts());

        List<Duration> ages = new ArrayList<>();
        ages.add(Duration.ZERO);
        Duration limit = policy.maxMessageAge();
        if (limit.compareTo(Duration.ofMillis(1)) > 0) {
            ages.add(limit.minusMillis(1));
            ages.add(limit);
        } else {
            // No limit, so there is no boundary to sit either side of — but "no limit" is a
            // claim worth a row of its own, and the age that proves it is the one that used to
            // fail. Java abandoned a message this old and the other four retried it.
            ages.add(A_YEAR);
        }

        for (int attempt : attempts) {
            for (Duration age : ages) {
                Map<String, Object> decision = new LinkedHashMap<>();
                decision.put("attempt", (long) attempt);
                decision.put("messageAgeMillis", age.toMillis());
                decision.put("retries", policy.nextWait(attempt, age).isPresent());
                decisions.add(decision);
            }
        }
        return decisions;
    }

    // ---------------------------------------------------------------- the age limit

    /**
     * When a message is too old to retry, at two limits and either side of the boundary.
     *
     * <p>The section exists because this is where Java disagreed with everybody. Its factories
     * set an age limit of a year that no caller had asked for and compared against it
     * unconditionally, so a message exactly a year old was dead-lettered by a Java consumer and
     * retried by the four ports, which read a limit of zero as no limit at all. Zero now means
     * the same thing in all five, and the rows below are computed by asking the library rather
     * than by writing down what it is believed to answer.
     */
    private static Map<String, Object> maxMessageAge() {
        List<Duration> ages = Arrays.asList(
                Duration.ZERO,
                Duration.ofMillis(119_999),
                Duration.ofMinutes(2),
                Duration.ofMillis(120_001),
                A_YEAR);

        List<Object> cases = new ArrayList<>();
        for (Duration limit : Arrays.asList(Duration.ZERO, Duration.ofMinutes(2))) {
            // Five attempts and a one-second wait, so nothing here is ever decided by the
            // attempt count: every row that stops is a row the age stopped.
            RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofSeconds(1)).giveUpAfter(limit);
            for (Duration age : ages) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("maxMessageAgeMillis", limit.toMillis());
                row.put("messageAgeMillis", age.toMillis());
                row.put("retries", policy.nextWait(1, age).isPresent());
                cases.add(row);
            }
        }

        Map<String, Object> section = new LinkedHashMap<>();
        section.put("defaultMillis", RetryPolicy.fixed(5, Duration.ofSeconds(1)).maxMessageAge().toMillis());
        section.put("zeroMeansNoLimit", true);
        section.put(
                "rule",
                "maxMessageAgeMillis of 0 means no age limit, and 0 is what every factory produces: only"
                        + " giveUpAfter sets a limit. A message is abandoned when its age reaches a non-zero"
                        + " limit, not when it passes it, so an age exactly equal to the limit does not retry."
                        + " An age that is not known counts as young enough");
        section.put("unknownAgeRetries", RetryPolicy.fixed(5, Duration.ofSeconds(1))
                .giveUpAfter(Duration.ofMinutes(2))
                .nextWait(1, null)
                .isPresent());
        section.put("cases", cases);
        return section;
    }

    // ---------------------------------------------------------------- jitter

    /**
     * The bounds a jittered wait must fall inside, and nothing that was rolled.
     *
     * <p>A port cannot compare a random number with a random number, so what is pinned is the
     * factor, the two multipliers it implies, the floor, and the fact that a wait held on a rung
     * queue is not jittered at all. Every one of those is checked here by sampling before it is
     * written, so the file cannot claim a symmetry the library does not have.
     */
    private static Map<String, Object> jitter() {
        RetryPolicy policy = RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofHours(24));
        double factor = policy.jitterFactor();

        Duration base = policy.schedule().get(0);
        long low = Long.MAX_VALUE;
        long high = Long.MIN_VALUE;
        for (int i = 0; i < 20_000; i++) {
            long sampled = policy.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new).delay().toMillis();
            low = Math.min(low, sampled);
            high = Math.max(high, sampled);
        }
        double lowest = base.toMillis() * (1 - factor);
        double highest = base.toMillis() * (1 + factor);
        if (low >= base.toMillis() || high <= base.toMillis()) {
            throw new IllegalStateException("jitter did not move the delay in both directions: saw " + low
                    + "ms to " + high + "ms around " + base.toMillis() + "ms");
        }
        if (low < lowest || high > highest) {
            throw new IllegalStateException("jitter left the bounds it claims: saw " + low + "ms to " + high
                    + "ms, bounds are " + lowest + "ms to " + highest + "ms");
        }

        // A wait the broker holds names a rung queue whose time-to-live was fixed at
        // declaration, so moving it would name a queue nobody declared. Proven rather than
        // asserted in prose: sample it and insist every answer is the schedule's own number.
        RetryPolicy inBroker = RetryPolicy.fixed(2, Duration.ofMinutes(1)).withJitter(factor);
        for (int i = 0; i < 1_000; i++) {
            RetryPolicy.Wait wait = inBroker.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new);
            if (!wait.isInBroker() || wait.delay().toMillis() != Duration.ofMinutes(1).toMillis()) {
                throw new IllegalStateException("a wait held in the broker was jittered: " + wait);
            }
        }

        // The floor. A delay small enough that a downward roll would reach zero is held at one
        // millisecond, because a zero-length wait is a hot loop rather than a retry.
        RetryPolicy tiny = RetryPolicy.fixed(2, Duration.ofMillis(1)).withJitter(1.0);
        long floor = Long.MAX_VALUE;
        for (int i = 0; i < 5_000; i++) {
            floor = Math.min(floor, tiny.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new)
                    .delay().toMillis());
        }

        Map<String, Object> jitter = new LinkedHashMap<>();
        jitter.put("policy", "exponential-uncapped");
        jitter.put("factor", factor);
        jitter.put("appliesInBothDirections", true);
        jitter.put("minMultiplier", 1 - factor);
        jitter.put("maxMultiplier", 1 + factor);
        jitter.put("appliesToConsumerWaits", true);
        jitter.put("appliesToBrokerWaits", false);
        jitter.put("flooredAtMillis", floor);
        jitter.put(
                "howToAssert",
                "a wait spent in the consumer must land in [delay * minMultiplier, delay * maxMultiplier] and"
                        + " never below flooredAtMillis; a wait spent in the broker must equal the scheduled"
                        + " delay exactly, because it names a rung queue");
        return jitter;
    }

    // ---------------------------------------------------------------- the threshold

    /** Which waits the consumer holds and which get a rung, at three thresholds. */
    private static Map<String, Object> brokerWaitThreshold() {
        List<Duration> delays = Arrays.asList(
                Duration.ZERO,
                Duration.ofMillis(1),
                Duration.ofMillis(500),
                Duration.ofSeconds(1),
                Duration.ofMillis(29_999),
                Duration.ofSeconds(30),
                Duration.ofMillis(30_001),
                Duration.ofSeconds(60),
                Duration.ofSeconds(90),
                Duration.ofMinutes(5));

        List<Object> cases = new ArrayList<>();
        for (Duration threshold : Arrays.asList(
                RetryPolicy.DEFAULT_BROKER_WAIT_THRESHOLD, Duration.ofSeconds(60), Duration.ZERO)) {
            RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofSeconds(1)).waitInBrokerFrom(threshold);
            for (Duration delay : delays) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("thresholdMillis", threshold.toMillis());
                row.put("delayMillis", delay.toMillis());
                row.put("waitsInBroker", policy.waitsInBroker(delay));
                cases.add(row);
            }
        }

        Map<String, Object> section = new LinkedHashMap<>();
        section.put("defaultMillis", RetryPolicy.DEFAULT_BROKER_WAIT_THRESHOLD.toMillis());
        section.put(
                "rule",
                "a wait at or above the threshold is held on a rung queue; anything below it, and anything"
                        + " zero or negative, is held in the consumer. A threshold of zero sends nothing to"
                        + " the broker, so no rung queue is ever declared");
        section.put("cases", cases);
        return section;
    }

    // ---------------------------------------------------------------- naming

    /** {@code {queue}.dlq}, {@code {queue}.parked} and {@code {queue}.retry.{delay}}. */
    private static Map<String, Object> naming() {
        RetryTopology plain = RetryTopology.forQueue(QUEUE, RetryPolicy.none());

        List<Object> retryQueues = new ArrayList<>();
        for (Duration delay : NAMED_DELAYS) {
            // One rung per call, and the threshold dropped to a millisecond so that even the
            // sub-second delay reaches the broker and can be named at all. Read back off the
            // topology rather than formatted here, so the fixture cannot disagree with the code
            // that declares the queue.
            RetryTopology one = RetryTopology.forQueue(
                    QUEUE, RetryPolicy.fixed(2, delay).waitInBrokerFrom(EVERYTHING_IN_THE_BROKER));
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("delayMillis", delay.toMillis());
            row.put("queue", one.rungs().get(delay));
            retryQueues.add(row);
        }

        Map<String, Object> naming = new LinkedHashMap<>();
        naming.put("queue", QUEUE);
        naming.put("deadLetterQueue", plain.deadLetterQueue());
        naming.put("parkedQueue", plain.parkingLotQueue());
        naming.put("retryQueues", retryQueues);
        naming.put(
                "subSecondDisagreement",
                "Java renders a sub-second delay in milliseconds — orders.new.retry.500ms — where the Go,"
                        + " Python and Ruby libraries render orders.new.retry.0s. Recorded rather than"
                        + " resolved. It is unreachable through the default thirty-second threshold and only"
                        + " appears for a policy that deliberately lowers the threshold below a second, but"
                        + " the two names are not the same queue and the libraries do not agree yet");
        return naming;
    }

    // ---------------------------------------------------------------- rung arguments

    /** Exactly the three arguments a rung queue must be declared with, and nothing else. */
    private static List<Object> rungArguments() {
        List<Object> rows = new ArrayList<>();
        for (Duration delay : NAMED_DELAYS) {
            RetryTopology one = RetryTopology.forQueue(
                    QUEUE, RetryPolicy.fixed(2, delay).waitInBrokerFrom(EVERYTHING_IN_THE_BROKER));
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("queue", one.rungs().get(delay));
            row.put("delayMillis", delay.toMillis());
            row.put("arguments", RetryTopology.rungArguments(QUEUE, delay));
            rows.add(row);
        }
        return rows;
    }

    // ---------------------------------------------------------------- topology

    /**
     * Everything declared for a queue that dead-letters and retries.
     *
     * <p>Two halves, and both are needed. The operator declares the source queue, its two
     * dead-letter queues and the exchange that reaches them, through {@link Topology}; the
     * consumer declares the rungs and the exchange that brings expired messages home, through
     * {@link RetryTopology#declare}. Neither half is the whole topology, and a port that
     * implements one of them has a broker that quietly drops messages.
     *
     * <p>The consumer's half is recorded by handing {@code declare} a connection that writes
     * down what it was asked for instead of doing it, so the fixture is the library's own
     * declaration calls rather than a second description of them.
     */
    private static Map<String, Object> topology() {
        RetryPolicy policy = RetryPolicy.exponential(6, Duration.ofSeconds(10), Duration.ofMinutes(5));

        Topology declared = Topology.define()
                .exchange("orders", "topic")
                .queueWithDeadLetter(QUEUE)
                .bind(QUEUE, "orders", "order.*")
                .build();

        Recorder recorder = new Recorder();
        RetryTopology.forQueue(QUEUE, policy).declare(recorder);

        List<Object> exchanges = new ArrayList<>();
        for (Topology.ExchangeSpec exchange : declared.exchanges()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", exchange.name());
            row.put("type", exchange.type());
            row.put("durable", exchange.durable());
            row.put("declaredBy", recorder.exchanges.containsKey(exchange.name()) ? "both" : "topology");
            exchanges.add(row);
        }
        for (Map.Entry<String, Recorder.Exchange> exchange : recorder.exchanges.entrySet()) {
            if (named(exchanges, exchange.getKey())) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", exchange.getKey());
            row.put("type", exchange.getValue().type);
            row.put("durable", exchange.getValue().durable);
            row.put("declaredBy", "consumer");
            exchanges.add(row);
        }

        List<Object> queues = new ArrayList<>();
        for (Topology.QueueSpec queue : declared.queues()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", queue.name());
            row.put("type", queue.quorum() ? "quorum" : "classic");
            row.put("durable", queue.durable());
            row.put("arguments", queue.arguments());
            row.put("declaredBy", recorder.queues.containsKey(queue.name()) ? "both" : "topology");
            queues.add(row);
        }
        for (Map.Entry<String, Recorder.Queue> queue : recorder.queues.entrySet()) {
            if (named(queues, queue.getKey())) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", queue.getKey());
            row.put("type", queue.getValue().type.name().toLowerCase(java.util.Locale.ROOT));
            row.put("durable", queue.getValue().durable);
            row.put("arguments", queue.getValue().arguments);
            row.put("declaredBy", "consumer");
            queues.add(row);
        }

        // Both halves bind the dead-letter and parking queues, which is not a mistake — the
        // declaration is idempotent and each half has to be correct on its own. Recorded once,
        // marked "both", because a fixture that listed it twice reads like a bug report.
        Map<String, Map<String, Object>> byKey = new LinkedHashMap<>();
        for (Topology.BindingSpec binding : declared.bindings()) {
            add(byKey, binding.queue(), binding.exchange(), binding.routingKey(), "topology");
        }
        for (String[] binding : recorder.bindings) {
            add(byKey, binding[0], binding[1], binding[2], "consumer");
        }
        List<Object> bindings = new ArrayList<>(byKey.values());

        Map<String, Object> section = new LinkedHashMap<>();
        section.put("sourceQueue", QUEUE);
        section.put("policy", "exponential(6, 10s, 5m) at the default threshold");
        section.put("policyScheduleMillis", millis(policy.schedule()));
        section.put("policyRungDelaysMillis", millis(policy.brokerRungs()));
        section.put("exchanges", exchanges);
        section.put("queues", queues);
        section.put("bindings", bindings);
        return section;
    }

    private static void add(
            Map<String, Map<String, Object>> bindings,
            String queue,
            String exchange,
            String routingKey,
            String by) {
        String key = queue + ' ' + exchange + ' ' + routingKey;
        Map<String, Object> existing = bindings.get(key);
        if (existing != null) {
            existing.put("declaredBy", existing.get("declaredBy").equals(by) ? by : "both");
            return;
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("queue", queue);
        row.put("exchange", exchange);
        row.put("routingKey", routingKey);
        row.put("declaredBy", by);
        bindings.put(key, row);
    }

    private static boolean named(List<Object> rows, String name) {
        for (Object row : rows) {
            @SuppressWarnings("unchecked")
            Map<String, Object> entry = (Map<String, Object>) row;
            if (name.equals(entry.get("name"))) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------- queue types

    /** Which declarations produce a quorum queue and which produce a classic one. */
    private static Map<String, Object> queueTypeDefaults() {
        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put("sourceQueue", type(Topology.define().queue("q"), "q"));
        defaults.put("sourceQueueWithDeadLetter", type(Topology.define().queueWithDeadLetter("q"), "q"));
        defaults.put(
                "explicitClassicQueue",
                type(Topology.define().classicQueue("q", Collections.emptyMap()), "q"));
        defaults.put(
                "explicitClassicQueueWithDeadLetter",
                type(Topology.define().classicQueueWithDeadLetter("q", Collections.emptyMap()), "q"));
        defaults.put("deadLetterQueue", type(Topology.define().queueWithDeadLetter("q"), "q.dlq"));
        defaults.put("parkedQueue", type(Topology.define().queueWithDeadLetter("q"), "q.parked"));

        Recorder recorder = new Recorder();
        RetryTopology.forQueue(
                QUEUE, RetryPolicy.fixed(2, Duration.ofMinutes(1))).declare(recorder);
        defaults.put(
                "retryRung",
                recorder.queues.get(QUEUE + ".retry.1m").type.name().toLowerCase(java.util.Locale.ROOT));

        Map<String, Object> durability = new LinkedHashMap<>();
        durability.put("everyDeclaredQueueIsDurable", everyQueueIsDurable());
        durability.put(
                "exclusiveAutoDeleteOrTransient",
                "not declarable through the Java API at all: Topology offers quorum, classic and"
                        + " dead-lettering variants and every one of them is durable, so the rule that an"
                        + " exclusive, auto-delete or transient queue stays classic has nothing to attach to"
                        + " here. The ports that do offer those flags must keep them classic; Java cannot be"
                        + " asked the question");
        defaults.put("durability", durability);
        return defaults;
    }

    private static String type(Topology.Builder builder, String queue) {
        for (Topology.QueueSpec spec : builder.build().queues()) {
            if (spec.name().equals(queue)) {
                return spec.quorum() ? "quorum" : "classic";
            }
        }
        throw new IllegalStateException("no queue named " + queue + " in that topology");
    }

    private static boolean everyQueueIsDurable() {
        Topology every = Topology.define()
                .queue("a")
                .classicQueue("b", Collections.emptyMap())
                .queueWithDeadLetter("c")
                .classicQueueWithDeadLetter("d", Collections.emptyMap())
                .build();
        for (Topology.QueueSpec spec : every.queues()) {
            if (!spec.durable()) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------- plumbing

    private static List<Object> millis(List<Duration> durations) {
        List<Object> out = new ArrayList<>();
        for (Duration duration : durations) {
            out.add(duration.toMillis());
        }
        return out;
    }

    /**
     * A connection that writes down what it was asked to declare and does none of it.
     *
     * <p>Nothing here talks to a broker, deliberately. The in-memory broker refuses a quorum
     * queue, which is the type the source queue has to be, so a fixture generated by declaring
     * against it could not contain the very fact it most needs to state.
     */
    private static final class Recorder implements TransportConnection {

        private final Map<String, Exchange> exchanges = new LinkedHashMap<>();
        private final Map<String, Queue> queues = new LinkedHashMap<>();
        private final List<String[]> bindings = new ArrayList<>();

        private static final class Exchange {

            private final String type;
            private final boolean durable;

            Exchange(String type, boolean durable) {
                this.type = type;
                this.durable = durable;
            }
        }

        private static final class Queue {

            private final QueueType type;
            private final boolean durable;
            private final Map<String, Object> arguments;

            Queue(QueueType type, boolean durable, Map<String, Object> arguments) {
                this.type = type;
                this.durable = durable;
                this.arguments = new LinkedHashMap<>(arguments);
            }
        }

        @Override
        public void declareExchange(String name, String type, boolean durable) {
            exchanges.put(name, new Exchange(type, durable));
        }

        @Override
        public void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments) {
            queues.put(name, new Queue(type, durable, arguments));
        }

        @Override
        public void bindQueue(String queue, String exchange, String routingKey) {
            bindings.add(new String[]{queue, exchange, routingKey});
        }

        @Override
        public ConfirmResult send(OutboundMessage message) {
            throw new UnsupportedOperationException("the recorder only records declarations");
        }

        @Override
        public Subscription subscribe(String queue, int prefetch, DeliveryListener listener) {
            throw new UnsupportedOperationException("the recorder only records declarations");
        }

        @Override
        public void deleteQueue(String name) {
            throw new UnsupportedOperationException("the recorder only records declarations");
        }

        @Override
        public boolean queueExists(String name) {
            return queues.containsKey(name);
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
            // Nothing was opened.
        }
    }

    /**
     * The smallest JSON writer that produces stable bytes.
     *
     * <p>Written here rather than pulled in, because the fixture is compared byte for byte and a
     * library that reorders keys, changes how it renders a double or decides to pretty-print
     * differently between versions would turn a dependency bump into a five-repository change.
     */
    private static final class Json {

        private Json() {
        }

        static String render(Object value) {
            StringBuilder out = new StringBuilder();
            write(out, value, 0);
            out.append('\n');
            return out.toString();
        }

        private static void write(StringBuilder out, Object value, int depth) {
            if (value instanceof Map) {
                writeObject(out, (Map<?, ?>) value, depth);
            } else if (value instanceof List) {
                writeArray(out, (List<?>) value, depth);
            } else {
                writeScalar(out, value);
            }
        }

        private static void writeObject(StringBuilder out, Map<?, ?> map, int depth) {
            if (map.isEmpty()) {
                out.append("{}");
                return;
            }
            out.append("{\n");
            int i = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                indent(out, depth + 1);
                writeScalar(out, String.valueOf(entry.getKey()));
                out.append(": ");
                write(out, entry.getValue(), depth + 1);
                out.append(++i < map.size() ? ",\n" : "\n");
            }
            indent(out, depth);
            out.append('}');
        }

        private static void writeArray(StringBuilder out, List<?> list, int depth) {
            if (list.isEmpty()) {
                out.append("[]");
                return;
            }
            // Scalars stay on one line. A schedule is read as a sequence and reads as one.
            boolean scalars = true;
            for (Object element : list) {
                scalars &= !(element instanceof Map) && !(element instanceof List);
            }
            if (scalars) {
                out.append('[');
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) {
                        out.append(", ");
                    }
                    writeScalar(out, list.get(i));
                }
                out.append(']');
                return;
            }
            out.append("[\n");
            for (int i = 0; i < list.size(); i++) {
                indent(out, depth + 1);
                write(out, list.get(i), depth + 1);
                out.append(i < list.size() - 1 ? ",\n" : "\n");
            }
            indent(out, depth);
            out.append(']');
        }

        private static void writeScalar(StringBuilder out, Object value) {
            if (value == null) {
                out.append("null");
            } else if (value instanceof Boolean || value instanceof Long || value instanceof Integer) {
                out.append(value);
            } else if (value instanceof Double || value instanceof Float) {
                // Shortest exact rendering, so 0.2 is "0.2" rather than "0.2000000000000000111".
                out.append(new BigDecimal(value.toString()).stripTrailingZeros().toPlainString());
            } else {
                out.append('"')
                        .append(String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\""))
                        .append('"');
            }
        }

        private static void indent(StringBuilder out, int depth) {
            for (int i = 0; i < depth; i++) {
                out.append("  ");
            }
        }
    }
}
