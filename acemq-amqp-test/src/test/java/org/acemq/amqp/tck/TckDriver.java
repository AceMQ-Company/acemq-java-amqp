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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.AceRetryableException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishingPausedException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.MicrometerSupport;
import org.acemq.amqp.core.PipelineBuilder;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.core.Replay;
import org.acemq.amqp.core.RequestTimedOutException;
import org.acemq.amqp.core.Requester;
import org.acemq.amqp.core.ResponderFailedException;
import org.acemq.amqp.core.StreamReader;
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;
import org.acemq.amqp.patterns.Scheduler;
import org.acemq.amqp.transport.ConnectionBlockedException;
import org.acemq.amqp.transport.ConnectionConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

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
            "schedule", "request", "declareStream", "readStream", "metrics", "replay", "pipeline");

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String url;
    private AceMq mq;
    private final Map<String, AutoCloseable> consumers = new LinkedHashMap<>();
    private final Map<String, ConcurrentMap<String, AtomicInteger>> counts = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentMap<String, AtomicInteger>> redelivered = new ConcurrentHashMap<>();
    private final Map<String, List<String>> order = new ConcurrentHashMap<>();
    private SimpleMeterRegistry recorder = new SimpleMeterRegistry();
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
            case "declareStream" :
                return declareStream(args);
            case "readStream" :
                return readStream(args);
            case "metrics" :
                return metrics();
            case "replay" :
                return replay(args);
            case "pipeline" :
                return pipeline(args);
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
        ConnectionConfig.Builder config = ConnectionConfig.url(url).clientName(clientName);
        if (Boolean.FALSE.equals(args.get("confirms"))) {
            config.withoutPublisherConfirms();
        }
        // The telemetry seam, recorded by the library's own Micrometer binding into a registry
        // that starts empty with each case.
        recorder = new SimpleMeterRegistry();
        mq = AceMq.connect(config.build(), MicrometerSupport.telemetry(recorder, "rabbitmq"));
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
        for (AutoCloseable consumer : consumers.values()) {
            consumer.close();
        }
        consumers.clear();
        counts.clear();
        redelivered.clear();
        order.clear();
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
        // Absent is the library's default, which is mandatory; true and false are explicit.
        if (Boolean.TRUE.equals(args.get("mandatory"))) {
            publisher = publisher.with(PublishOptions.defaults());
        } else if (Boolean.FALSE.equals(args.get("mandatory"))) {
            publisher = publisher.with(PublishOptions.defaults().allowUnroutable());
        }
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
        Map<?, ?> behaviour = (Map<?, ?>) args.getOrDefault("behaviour", Map.of());
        Map<?, ?> failFirst = failFirst(behaviour);
        long sleepMs = intOf(behaviour.get("sleepMs"), 0);
        Counter counter = track(handle);

        // No retry argument is no retry policy: the consumer exactly as the library builds it
        // when the caller configures none.
        ConsumerOptions options = ConsumerOptions.prefetch(intOf(args.get("prefetch"), 10));
        if (args.get("retry") instanceof Map) {
            Map<?, ?> retry = (Map<?, ?>) args.get("retry");
            options = options.withRetry(RetryPolicy.fixed(
                    intOf(retry.get("maxAttempts"), 3), Duration.ofMillis(intOf(retry.get("delayMs"), 1000))));
        }
        if (Boolean.TRUE.equals(args.get("idempotent"))) {
            options = options.idempotent(InMemoryIdempotencyStore.forOneDay());
        }

        // The source queue with its dead-letter and parked queues, as the library's topology
        // declares a consumed queue; the consumer declares the retry rungs its policy needs.
        mq().topology().apply(Topology.define().queueWithDeadLetter(queue).build(), ApplyMode.CREATE_ONLY);
        MessageConsumer consumer = mq().consume(
                queue,
                Object.class,
                options,
                message -> {
                    String id = idOf(message.payload());
                    int delivery = counter.count(id, message.redelivered());
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
                    }
                    failIfAsked(failFirst, id, delivery);
                });
        consumers.put(handle, consumer);
        return Map.of();
    }

    /** Counts deliveries under one handle, read back by {@code handled}. */
    private interface Counter {
        int count(String id, boolean redelivered);
    }

    private Counter track(String handle) {
        ConcurrentMap<String, AtomicInteger> seen = new ConcurrentHashMap<>();
        ConcurrentMap<String, AtomicInteger> again = new ConcurrentHashMap<>();
        List<String> arrived = new CopyOnWriteArrayList<>();
        counts.put(handle, seen);
        redelivered.put(handle, again);
        order.put(handle, arrived);
        return (id, wasRedelivered) -> {
            int delivery = seen.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
            arrived.add(id);
            if (wasRedelivered) {
                again.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
            }
            return delivery;
        };
    }

    private static Map<?, ?> failFirst(Map<?, ?> behaviour) {
        return behaviour.get("failFirst") instanceof Map ? (Map<?, ?>) behaviour.get("failFirst") : Map.of();
    }

    private static void failIfAsked(Map<?, ?> failFirst, String id, int delivery) {
        if (delivery <= intOf(failFirst.get(id), 0)) {
            throw new AceRetryableException("tck: failing " + id + " on purpose, delivery " + delivery);
        }
    }

    private static String idOf(Object payload) {
        return String.valueOf(payload instanceof Map ? ((Map<?, ?>) payload).get("id") : null);
    }

    private Map<String, Object> declareStream(Map<String, Object> args) {
        Object maxAgeMs = args.get("maxAgeMs");
        mq().declareStream(
                String.valueOf(args.get("queue")),
                maxAgeMs instanceof Number ? Duration.ofMillis(((Number) maxAgeMs).longValue()) : null,
                null);
        return Map.of();
    }

    private Map<String, Object> readStream(Map<String, Object> args) {
        String handle = String.valueOf(args.get("handle"));
        Map<?, ?> failFirst = args.get("behaviour") instanceof Map
                ? failFirst((Map<?, ?>) args.get("behaviour"))
                : Map.of();
        Counter counter = track(handle);
        StreamReader<Object> reader = mq().stream(String.valueOf(args.get("queue")), Object.class);
        reader = "next".equals(args.get("offset")) ? reader.fromNext() : reader.fromFirst();
        // The library's default handling of a failed handler, which stops the reader.
        AutoCloseable consumer = reader.consume(message -> {
            String id = idOf(message.payload());
            failIfAsked(failFirst, id, counter.count(id, false));
        });
        consumers.put(handle, consumer);
        return Map.of();
    }

    /**
     * Every meter the library reported since {@code begin}. Micrometer keeps one meter per name
     * and tag set rather than one entry per report, so each distinct pair is one sample.
     */
    private Map<String, Object> metrics() {
        List<Map<String, Object>> samples = new ArrayList<>();
        java.util.SortedSet<String> names = new java.util.TreeSet<>();
        for (Meter meter : recorder.getMeters()) {
            Map<String, String> tags = new LinkedHashMap<>();
            for (Tag tag : meter.getId().getTags()) {
                tags.put(tag.getKey(), tag.getValue());
            }
            names.add(meter.getId().getName());
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("name", meter.getId().getName());
            sample.put("tags", tags);
            samples.add(sample);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("names", new ArrayList<>(names));
        result.put("samples", samples);
        return result;
    }

    /**
     * One filtered replay with the library's defaults, answered the moment it returns. Java's
     * replay is addressed by the source queue and reads its {@code .dlq}, or {@code .parked}.
     */
    private Map<String, Object> replay(Map<String, Object> args) {
        String from = String.valueOf(args.get("from"));
        List<?> ids = (List<?>) args.getOrDefault("ids", List.of());
        Replay replay = mq().replay(String.valueOf(args.get("queue")));
        if (from.endsWith(".parked")) {
            replay = replay.parked();
        }
        int replayed = replay.replay(Integer.MAX_VALUE, delivery -> {
            try {
                return ids.contains(JSON.readTree(delivery.body()).path("id").textValue());
            } catch (java.io.IOException notJson) {
                return false;
            }
        });
        return Map.of("replayed", replayed);
    }

    /**
     * A pipeline whose every step is the identity on the JSON body, counting {@code body.id}.
     * The library's builder declares the exchange, the step queues and their bindings itself.
     */
    private Map<String, Object> pipeline(Map<String, Object> args) {
        String name = String.valueOf(args.get("name"));
        Counter counter = track(String.valueOf(args.get("handle")));
        PipelineBuilder<Object, Object> builder = mq().pipeline(name, Object.class);
        for (Object step : (List<?>) args.getOrDefault("steps", List.of())) {
            builder = builder.step(String.valueOf(step), Object.class, message -> {
                counter.count(idOf(message.payload()), message.redelivered());
                return message.payload();
            });
        }
        consumers.put("pipeline:" + name, builder.build());
        return Map.of();
    }

    private Map<String, Object> handled(Map<String, Object> args) {
        String handle = String.valueOf(args.get("handle"));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("counts", snapshot(counts.get(handle)));
        result.put("redelivered", snapshot(redelivered.get(handle)));
        result.put("order", new ArrayList<>(order.getOrDefault(handle, List.of())));
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
     * plain {@code AceMqException}. Unroutable and nacked publishes are the same exception
     * type in this library, told apart by its {@code isUnroutable()} field. A
     * publish declined under back pressure is a blocked connection known before the write, the
     * same test the publisher makes when it counts the outcome as refused.
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
            if (at instanceof ResponderFailedException) {
                return "responder";
            }
            if (at instanceof PublishingPausedException
                    || (at instanceof ConnectionBlockedException
                            && !((ConnectionBlockedException) at).mayHaveBeenPublished())) {
                return "paused";
            }
            if (at instanceof PublishFailedException) {
                return ((PublishFailedException) at).isUnroutable() ? "unroutable" : "nacked";
            }
            if (at instanceof AceFatalException) {
                return "decode";
            }
        }
        return "other";
    }
}
