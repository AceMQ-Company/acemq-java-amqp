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
package org.acemq.amqp.tck;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.AceRetryableException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.RequestTimedOutException;
import org.acemq.amqp.core.Requester;
import org.acemq.amqp.patterns.Scheduler;
import org.acemq.amqp.transport.ConnectionConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The AceMQ TCK driver for this library.
 *
 * <p>Started once by the TCK runner (github.com/AceMQ-Company/acemq-amqp-tck), which writes one
 * JSON request per line to stdin and reads one JSON answer per line from stdout. Every operation
 * goes through the public API with default settings; nothing here asserts, because the runner
 * checks the outcome on the broker. The protocol is docs/driver-contract.md in the TCK
 * repository, and this mirrors the Python reference driver.
 *
 * <pre>
 * mvn -q -pl acemq-amqp-test -am test-compile dependency:build-classpath \
 *     -Dmdep.outputFile=target/tck.classpath -Dmdep.includeScope=test
 * java -cp "acemq-amqp-test/target/test-classes:$(cat acemq-amqp-test/target/tck.classpath)" \
 *     org.acemq.amqp.tck.TckDriver
 * </pre>
 */
public final class TckDriver {

    private static final List<String> OPS = List.of("hello", "begin", "end", "publish", "consume", "handled",
            "schedule", "request");

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String url;
    private AceMq mq;
    private final Map<String, MessageConsumer> consumers = new LinkedHashMap<>();
    private final Map<String, ConcurrentMap<String, AtomicInteger>> counts = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentMap<String, AtomicInteger>> redelivered = new ConcurrentHashMap<>();
    private Scheduler scheduler;
    private Requester requester;

    private TckDriver(String url) {
        this.url = url;
    }

    public static void main(String[] args) throws Exception {
        // The protocol owns stdout. Anything else that writes there, the logger included, is
        // sent to stderr instead.
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        System.setOut(System.err);

        String url = System.getenv().getOrDefault("ACEMQ_TCK_AMQP_URL", "amqp://guest:guest@localhost:5672/");
        TckDriver driver = new TckDriver(url);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> request = JSON.readValue(line, Map.class);
            Map<String, Object> answer = new LinkedHashMap<>();
            answer.put("id", request.get("id"));
            try {
                answer.put("result", driver.dispatch(String.valueOf(request.get("op")), request));
                answer.put("ok", true);
            } catch (Exception failure) {
                Map<String, Object> error = new LinkedHashMap<>();
                error.put("kind", kindOf(failure));
                error.put("type", failure.getClass().getSimpleName());
                error.put("message", String.valueOf(failure.getMessage()));
                answer.put("ok", false);
                answer.put("error", error);
            }
            out.println(JSON.writeValueAsString(answer));
        }
        driver.end();
        System.exit(0);
    }

    private Map<String, Object> dispatch(String op, Map<String, Object> args) throws Exception {
        switch (op) {
            case "hello" :
                return hello();
            case "begin" :
                return begin(args);
            case "end" :
                return end();
            case "publish" :
                return publish(args);
            case "consume" :
                return consume(args);
            case "handled" :
                return handled(args);
            case "schedule" :
                return schedule(args);
            case "request" :
                return request(args);
            default :
                throw new UnsupportedOperationException(op);
        }
    }

    private AceMq mq() {
        if (mq == null) {
            throw new IllegalStateException("begin was not called");
        }
        return mq;
    }

    private static Map<String, Object> hello() {
        Package where = AceMq.class.getPackage();
        String version = where == null ? null : where.getImplementationVersion();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("library", "acemq-java-amqp");
        result.put("version", version == null ? "unknown" : version);
        result.put("ops", OPS);
        return result;
    }

    private Map<String, Object> begin(Map<String, Object> args) throws Exception {
        end();
        String clientName = String.valueOf(args.getOrDefault("clientName", "tck"));
        mq = AceMq.connect(ConnectionConfig.url(url).clientName(clientName).build());
        return Map.of();
    }

    private Map<String, Object> end() throws Exception {
        if (requester != null) {
            requester.close();
            requester = null;
        }
        if (scheduler != null) {
            scheduler.close();
            scheduler = null;
        }
        for (MessageConsumer consumer : consumers.values()) {
            consumer.close();
        }
        consumers.clear();
        counts.clear();
        redelivered.clear();
        if (mq != null) {
            mq.close();
            mq = null;
        }
        return Map.of();
    }

    private Map<String, Object> publish(Map<String, Object> args) {
        String exchange = String.valueOf(args.getOrDefault("exchange", ""));
        String routingKey = String.valueOf(args.get("routingKey"));
        Object body = args.get("body");
        Map<?, ?> fields = (Map<?, ?>) args.get("envelope");
        var publisher = mq().publisher(exchange, routingKey);
        if (fields == null || fields.isEmpty()) {
            return Map.of("messageId", publisher.send(body).messageId());
        }
        // Java's envelope requires a type. When a case leaves it out, the routing key stands in,
        // which is what the publisher itself uses for a send without an envelope.
        Envelope.Builder envelope = Envelope
                .of(String.valueOf(fields.containsKey("type") ? fields.get("type") : routingKey));
        if (fields.get("id") != null) {
            envelope.id(String.valueOf(fields.get("id")));
        }
        if (fields.get("version") != null) {
            envelope.version(((Number) fields.get("version")).intValue());
        }
        if (fields.get("correlationId") != null) {
            envelope.correlationId(String.valueOf(fields.get("correlationId")));
        }
        if (fields.get("causationId") != null) {
            envelope.causationId(String.valueOf(fields.get("causationId")));
        }
        if (fields.get("headers") instanceof Map) {
            ((Map<?, ?>) fields.get("headers")).forEach((name, value) -> envelope.header(String.valueOf(name), value));
        }
        return Map.of("messageId", publisher.send(body, envelope.build()).messageId());
    }

    private Map<String, Object> consume(Map<String, Object> args) {
        String queue = String.valueOf(args.get("queue"));
        String handle = String.valueOf(args.get("handle"));
        Map<?, ?> retry = (Map<?, ?>) args.getOrDefault("retry", Map.of());
        RetryPolicy policy = RetryPolicy.fixed(
                intOf(retry.get("maxAttempts"), 3), Duration.ofMillis(intOf(retry.get("delayMs"), 1000)));
        Map<?, ?> behaviour = (Map<?, ?>) args.getOrDefault("behaviour", Map.of());
        Map<?, ?> failFirst = behaviour.get("failFirst") instanceof Map
                ? (Map<?, ?>) behaviour.get("failFirst")
                : Map.of();
        long sleepMs = intOf(behaviour.get("sleepMs"), 0);
        ConcurrentMap<String, AtomicInteger> seen = new ConcurrentHashMap<>();
        ConcurrentMap<String, AtomicInteger> again = new ConcurrentHashMap<>();

        // The source queue with its dead-letter and parked queues, as the library's topology
        // declares a consumed queue; the consumer declares the retry rungs its policy needs.
        mq().topology().apply(Topology.define().queueWithDeadLetter(queue).build(), ApplyMode.CREATE_ONLY);
        MessageConsumer consumer = mq().consume(
                queue,
                Object.class,
                ConsumerOptions.prefetch(intOf(args.get("prefetch"), 10)).withRetry(policy),
                message -> {
                    Object payload = message.payload();
                    String id = String.valueOf(payload instanceof Map ? ((Map<?, ?>) payload).get("id") : null);
                    int delivery = seen.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
                    if (message.redelivered()) {
                        again.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
                    }
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
                    }
                    if (delivery <= intOf(failFirst.get(id), 0)) {
                        throw new AceRetryableException("tck: failing " + id + " on purpose, delivery " + delivery);
                    }
                });
        consumers.put(handle, consumer);
        counts.put(handle, seen);
        redelivered.put(handle, again);
        return Map.of();
    }

    private Map<String, Object> handled(Map<String, Object> args) {
        String handle = String.valueOf(args.get("handle"));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("counts", snapshot(counts.get(handle)));
        result.put("redelivered", snapshot(redelivered.get(handle)));
        return result;
    }

    private Map<String, Object> schedule(Map<String, Object> args) {
        if (scheduler == null) {
            scheduler = Scheduler.on(mq());
        }
        scheduler.in(
                Duration.ofMillis(intOf(args.get("delayMs"), 0)),
                String.valueOf(args.getOrDefault("exchange", "")),
                String.valueOf(args.get("routingKey")),
                args.get("body"));
        return Map.of();
    }

    private Map<String, Object> request(Map<String, Object> args) {
        // Java's requester is one per connection, not one per destination: it owns the reply
        // queue and takes the destination on each call.
        if (requester == null) {
            requester = mq().requester();
        }
        Object reply = requester.request(
                String.valueOf(args.getOrDefault("exchange", "")),
                String.valueOf(args.get("routingKey")),
                args.get("body"),
                Object.class,
                Duration.ofMillis(intOf(args.get("timeoutMs"), 30_000)));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("reply", reply);
        return result;
    }

    private static Map<String, Integer> snapshot(Map<String, AtomicInteger> source) {
        Map<String, Integer> copy = new LinkedHashMap<>();
        if (source != null) {
            source.forEach((id, count) -> copy.put(id, count.get()));
        }
        return copy;
    }

    private static int intOf(Object value, int fallback) {
        return value instanceof Number ? ((Number) value).intValue() : fallback;
    }

    /**
     * Normalises a failure into the contract's closed list of kinds.
     *
     * <p>The cause chain is walked because the requester wraps a reply's decode failure in a
     * plain {@code AceMqException}. Unroutable and refused publishes are the same exception
     * type in this library, so they are told apart by the message the publisher writes.
     */
    static String kindOf(Throwable failure) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable at = failure; at != null && !chain.contains(at); at = at.getCause()) {
            chain.add(at);
        }
        for (Throwable at : chain) {
            if (at instanceof UnsupportedOperationException) {
                return "unsupported";
            }
            if (at instanceof RequestTimedOutException) {
                return "timeout";
            }
            if (at instanceof PublishFailedException) {
                return String.valueOf(at.getMessage()).contains("could not be routed") ? "unroutable" : "refused";
            }
            if (at instanceof AceFatalException) {
                return "decode";
            }
        }
        return "other";
    }
}
