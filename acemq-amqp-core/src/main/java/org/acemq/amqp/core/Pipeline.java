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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.api.RoutingSlip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A chain of steps, each its own queue.
 *
 * <pre>{@code
 * try (Pipeline<Order> fulfilment = mq.pipeline("fulfilment", Order.class)
 *         .step("validate", Order.class, new ValidateOrder())
 *         .step("enrich", Enriched.class, new EnrichOrder())
 *         .step("dispatch", Void.class, new DispatchOrder())
 *         .build()) {
 *
 *     fulfilment.send(order);
 * }
 * }</pre>
 *
 * <p>Every hop goes through the broker rather than through a method call, and that is the
 * decision everything else follows from. Chaining handlers in one process is function
 * composition wearing a messaging costume: a crash loses the work, a slow step blocks the chain,
 * and scaling is all or nothing. With a queue between two steps, a crash leaves the message
 * where it was, a slow step grows its own queue while the others carry on, and the step that
 * needs ten consumers gets ten while its neighbour keeps one.
 *
 * <p>The cost is a network round trip and a durable write per hop. Three steps is three
 * publishes and three confirms. Where each step is microseconds of CPU, a method call is the
 * right tool and this is not.
 *
 * <p>Where a message is going travels with it — see {@link RoutingSlip}. Nothing here
 * coordinates; each step reads the slip and publishes to whatever is next.
 *
 * @param <T> payload the pipeline is entered with
 */
public final class Pipeline<T> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private final AceMq mq;
    private final String name;
    private final Class<T> entryType;
    private final List<PipelineStep<?, ?>> steps;

    private final Map<String, Publisher<Object>> publishers = new LinkedHashMap<>();
    private final Map<String, ConsumerGroup> groups = new LinkedHashMap<>();
    private final AtomicLong entered = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong endedEarly = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    Pipeline(AceMq mq, String name, Class<T> entryType, List<PipelineStep<?, ?>> steps) {
        this.mq = mq;
        this.name = name;
        this.entryType = entryType;
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
    }

    /** @return the pipeline's name, which is also its exchange */
    public String name() {
        return name;
    }

    /** @return the step names, in order */
    public List<String> stepNames() {
        List<String> names = new ArrayList<>(steps.size());
        for (PipelineStep<?, ?> step : steps) {
            names.add(step.name());
        }
        return names;
    }

    /** @return the queue backing a step */
    public String queueFor(String step) {
        return name + "." + step;
    }

    /** Declares the exchange, one queue per step, and the bindings. */
    void declareTopology() {
        mq.declareExchange(name, "direct");

        // Replicated where the broker can. A pipeline queue holds work that has already passed
        // earlier steps, so losing the node holding it loses partly-finished runs — and the
        // steps that already succeeded would have to be repeated, which is exactly what a
        // non-idempotent first step cannot survive.
        org.acemq.amqp.transport.QueueType type = mq.supports(org.acemq.amqp.api.Capability.QUORUM_QUEUES)
                ? org.acemq.amqp.transport.QueueType.QUORUM
                : org.acemq.amqp.transport.QueueType.CLASSIC;

        for (PipelineStep<?, ?> step : steps) {
            String queue = queueFor(step.name());
            mq.declareQueue(queue, type, java.util.Collections.emptyMap());
            mq.bind(queue, name, step.name());
        }
        // Labels rather than bare names: this line is the one somebody reads to find out what
        // a pipeline actually does, and a described step says so here without being looked up.
        List<String> labels = new ArrayList<>();
        for (PipelineStep<?, ?> step : steps) {
            labels.add(step.label());
        }
        log.info("pipeline {} declared with {} steps: {}", name, steps.size(), String.join(" | ", labels));
    }

    /** Starts a consumer group per step. */
    void startConsumers() {
        for (int index = 0; index < steps.size(); index++) {
            PipelineStep<?, ?> step = steps.get(index);
            int position = index;
            ConsumerGroup group = mq.consumeGroup(
                    queueFor(step.name()), step.inputType(), message -> run(position, message))
                    .options(step.options())
                    .concurrency(step.concurrency())
                    .start();
            groups.put(step.name(), group);
        }
    }

    /**
     * Sends a payload in at the first step.
     *
     * @param payload the message
     * @return the identifier of this run, which every hop carries and which survives a
     *     dead-letter and a replay
     */
    public String send(T payload) {
        return send(payload, Envelope.of(name).build());
    }

    /**
     * @param payload the message
     * @param envelope metadata to carry, for correlation with whatever caused this
     * @return the run identifier
     */
    public String send(T payload, Envelope envelope) {
        requireOpen();
        RoutingSlip slip = RoutingSlip.startOf(stepNames());
        publishTo(steps.get(0).name(), payload, envelope, slip);
        entered.incrementAndGet();
        return slip.runId();
    }

    @SuppressWarnings("unchecked")
    private void run(int position, Message<?> message) throws Exception {
        PipelineStep<Object, Object> step = (PipelineStep<Object, Object>) steps.get(position);

        // The slip on the message wins over this consumer's position in the declaration. A
        // message replayed into the middle of a pipeline carries where it was going, and that
        // is what makes a replay resume rather than restart.
        RoutingSlip slip = message.envelope().route()
                .orElseGet(() -> RoutingSlip.startOf(stepNames()).advanceTo(position));

        Object next = step.handler().handle((Message<Object>) message);
        Optional<String> following = slip.next();

        // Whether there is a step after this one is asked first, and the order matters. The
        // last step of a route is almost always a terminal action with nothing to return, so
        // reading its null as "ended early" would report every completed run as a filtered one.
        if (!following.isPresent()) {
            completed.incrementAndGet();
            // The age of the envelope, so this is the whole run rather than this step: the
            // envelope was created when the message entered and carried through every hop.
            mq.telemetry().pipelineRunFinished(
                    name, step.name(), MetricNames.OUTCOME_COMPLETED, message.envelope().age());
            return;
        }

        if (next == null) {
            // Stopped before the end of the route: a decision, not a failure. Counted apart
            // from both so that "how many were filtered out" needs no log reading.
            endedEarly.incrementAndGet();
            mq.telemetry().pipelineRunFinished(
                    name, step.name(), MetricNames.OUTCOME_ENDED_EARLY, message.envelope().age());
            log.debug("run {} ended at step {} of pipeline {}", slip.runId(), step.label(), name);
            return;
        }

        publishTo(following.get(), next, message.envelope(), slip.advance());
    }

    private void publishTo(String step, Object payload, Envelope envelope, RoutingSlip slip) {
        Envelope carrying = envelope.toBuilder().route(slip).build();
        publisherFor(step).send(payload, carrying);
    }

    @SuppressWarnings("unchecked")
    private Publisher<Object> publisherFor(String destination) {
        // Publishers are meant to be long lived, and a pipeline publishes to the same handful
        // of routing keys forever.
        return publishers.computeIfAbsent(destination, key -> {
            DefaultPublisher<Object> publisher = mq.publisher(name, key, (Class<Object>) (Class<?>) Object.class);

            // encodedAs configures how a step publishes its own output, so the format for a
            // message arriving at D is the one configured on the step before D — not on D
            // itself. Reading it off the destination was a real bug: it applied enrich's
            // encoding to the messages enrich received rather than to the ones it sent.
            return encodingBefore(key).<Publisher<Object>>map(publisher::as).orElse(publisher);
        });
    }

    private java.util.Optional<org.acemq.amqp.api.Codec> encodingBefore(String destination) {
        for (int index = 1; index < steps.size(); index++) {
            if (steps.get(index).name().equals(destination)) {
                return steps.get(index - 1).codec();
            }
        }
        // The first step has no predecessor, so a message entering the pipeline is encoded
        // with whatever the connection publishes in.
        return java.util.Optional.empty();
    }

    private PipelineStep<?, ?> stepNamed(String step) {
        for (PipelineStep<?, ?> candidate : steps) {
            if (candidate.name().equals(step)) {
                return candidate;
            }
        }
        throw new AceMqException("pipeline " + name + " has no step called '" + step + "'. Its steps are "
                + String.join(", ", stepNames()) + ".");
    }

    /**
     * The consumers running one step, so it can be scaled independently.
     *
     * <p>The operational argument for putting queues between steps at all: a pipeline's slowest
     * step is its throughput, and this is how that one step gets more consumers without touching
     * the others.
     *
     * @param step step name
     * @return its consumer group
     */
    public ConsumerGroup step(String step) {
        ConsumerGroup group = groups.get(step);
        if (group == null) {
            throw new AceMqException("pipeline " + name + " has no running step called '" + step + "'. Its steps"
                    + " are " + String.join(", ", stepNames()) + ".");
        }
        return group;
    }

    /** @return how many messages have been sent into this pipeline by this instance */
    public long entered() {
        return entered.get();
    }

    /** @return how many runs reached the end of the route */
    public long completed() {
        return completed.get();
    }

    /** @return how many runs were ended early by a step returning nothing */
    public long endedEarly() {
        return endedEarly.get();
    }

    /** @return how many messages every step is handling right now */
    public long inFlight() {
        long total = 0;
        for (ConsumerGroup group : groups.values()) {
            total += group.inFlight();
        }
        return total;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new AceMqException("pipeline " + name + " is closed");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        groups.values().forEach(ConsumerGroup::close);
        groups.clear();
        publishers.values().forEach(Publisher::close);
        publishers.clear();
    }

    @Override
    public String toString() {
        return "Pipeline{" + name + ": " + String.join(" | ", stepNames()) + "}";
    }

    /** @return the type the pipeline is entered with */
    public Class<T> entryType() {
        return entryType;
    }
}
