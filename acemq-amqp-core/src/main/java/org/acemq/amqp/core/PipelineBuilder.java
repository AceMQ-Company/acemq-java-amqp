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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.IdempotencyStore;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Step;

/**
 * Declares the steps of a pipeline, in order, threading the types along the chain.
 *
 * <pre>{@code
 * mq.pipeline("fulfilment", Order.class)
 *         .step("validate", Order.class, new ValidateOrder())
 *         .step("enrich", Enriched.class, new EnrichOrder())
 *             .withRetry(RetryPolicy.exponential(5, Duration.ofSeconds(2), Duration.ofMinutes(1)))
 *             .concurrency(10)
 *         .step("dispatch", Void.class, new DispatchOrder())
 *         .build();
 * }</pre>
 *
 * <p>The type parameter moves with each step, so a {@code Step<Order, Enriched>} cannot be put
 * where one producing an {@code Order} belongs. That is the reason this is a builder rather than
 * a list of names: a typo in a string is found in production, and a type error is found by the
 * compiler.
 *
 * <p>The settings after a {@code step} apply to <em>that</em> step. They read as a chain because
 * steps fail differently, and giving a pipeline one retry policy would mean giving the step that
 * should never retry the same treatment as the one that always should.
 *
 * @param <T> payload the pipeline is entered with
 * @param <C> payload the step declared most recently produces
 */
public final class PipelineBuilder<T, C> {

    private final AceMq mq;
    private final String name;
    private final Class<T> entryType;
    private final List<PipelineStep<?, ?>> steps;

    PipelineBuilder(AceMq mq, String name, Class<T> entryType, List<PipelineStep<?, ?>> steps) {
        this.mq = mq;
        this.name = name;
        this.entryType = entryType;
        this.steps = steps;
    }

    /**
     * Adds a step to the end of the chain.
     *
     * @param stepName how the step is named on the wire and in the routing slip
     * @param produces what this step's handler returns, which the next step receives
     * @param handler the work
     * @param <O> produced type
     * @return a builder whose next step receives {@code O}
     */
    public <O> PipelineBuilder<T, O> step(String stepName, Class<O> produces, Step<C, O> handler) {
        Objects.requireNonNull(stepName, "stepName");
        Objects.requireNonNull(produces, "produces");
        Objects.requireNonNull(handler, "handler");
        if (!isRoutable(stepName)) {
            throw new IllegalArgumentException("a step name goes on the wire as a routing key and into the"
                    + " routing slip, so '" + stepName + "' will not do: use letters, digits, dashes and dots,"
                    + " and no commas.");
        }
        for (PipelineStep<?, ?> existing : steps) {
            if (existing.name().equals(stepName)) {
                throw new IllegalArgumentException("pipeline " + name + " already has a step called '" + stepName
                        + "'. Step names are the routing slip, so two of them would be ambiguous.");
            }
        }

        @SuppressWarnings("unchecked")
        Class<C> receives = (Class<C>) (steps.isEmpty() ? entryType : lastProduced());
        List<PipelineStep<?, ?>> updated = new ArrayList<>(steps);
        updated.add(new PipelineStep<>(stepName, receives, produces, handler, ConsumerOptions.defaults(), 1, null));
        return new PipelineBuilder<>(mq, name, entryType, updated);
    }

    /**
     * Retries the most recently declared step on a schedule, using the broker's queues rather
     * than a sleeping handler.
     *
     * @param policy the schedule
     * @return this builder
     */
    public PipelineBuilder<T, C> withRetry(RetryPolicy policy) {
        return replaceLast(last -> last.with(last.options().withRetry(policy), last.concurrency(),
                last.codec().orElse(null)));
    }

    /**
     * Handles each message at most once at the most recently declared step.
     *
     * <p>Worth configuring on any step whose work would matter twice. Each hop is at-least-once,
     * so a step sees a message again whenever a publish succeeded and its acknowledgement did
     * not.
     *
     * @param store where handled identifiers are remembered
     * @return this builder
     */
    public PipelineBuilder<T, C> idempotent(IdempotencyStore store) {
        return replaceLast(last -> last.with(last.options().idempotent(store), last.concurrency(),
                last.codec().orElse(null)));
    }

    /**
     * @param howMany consumers to run for the most recently declared step. This is how a slow
     *     step is given more capacity without touching its neighbours
     * @return this builder
     */
    public PipelineBuilder<T, C> concurrency(int howMany) {
        if (howMany < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1, was " + howMany);
        }
        return replaceLast(last -> last.with(last.options(), howMany, last.codec().orElse(null)));
    }

    /**
     * @param prefetch how many messages each consumer of the most recent step may hold
     * @return this builder
     */
    public PipelineBuilder<T, C> prefetch(int prefetch) {
        return replaceLast(last -> last.with(last.options().prefetch(prefetch), last.concurrency(),
                last.codec().orElse(null)));
    }

    /**
     * Publishes the most recent step's output in a named format.
     *
     * <p>A format change is not a step. Making it one would add a queue, a publish, a confirm
     * and a consumer whose entire job is to deserialize and reserialize the same object — a
     * network round trip to change nothing but the encoding.
     *
     * @param codec the format for this step's output
     * @return this builder
     */
    public PipelineBuilder<T, C> encodedAs(Codec codec) {
        Objects.requireNonNull(codec, "codec");
        return replaceLast(last -> last.with(last.options(), last.concurrency(), codec));
    }

    /**
     * Declares the topology and starts a consumer group per step.
     *
     * @return the running pipeline
     */
    public Pipeline<T> build() {
        if (steps.isEmpty()) {
            throw new IllegalStateException("pipeline " + name + " has no steps. A pipeline with nothing in it is"
                    + " an exchange nobody publishes to.");
        }
        Pipeline<T> pipeline = new Pipeline<>(mq, name, entryType, steps);
        pipeline.declareTopology();
        pipeline.startConsumers();
        return pipeline;
    }

    private Class<?> lastProduced() {
        // The next step receives whatever the previous produced. The builder carries that in C
        // for the compiler; this is the runtime class, which the consumer needs to decode with.
        return steps.get(steps.size() - 1).outputType();
    }

    private PipelineBuilder<T, C> replaceLast(java.util.function.UnaryOperator<PipelineStep<?, ?>> change) {
        if (steps.isEmpty()) {
            throw new IllegalStateException("there is no step to configure yet: call step(...) first");
        }
        List<PipelineStep<?, ?>> updated = new ArrayList<>(steps);
        updated.set(updated.size() - 1, change.apply(updated.get(updated.size() - 1)));
        return new PipelineBuilder<>(mq, name, entryType, updated);
    }

    private static boolean isRoutable(String stepName) {
        if (stepName.isEmpty()) {
            return false;
        }
        Set<Character> allowedPunctuation = new LinkedHashSet<>(java.util.Arrays.asList('-', '.', '_'));
        for (char c : stepName.toCharArray()) {
            if (!Character.isLetterOrDigit(c) && !allowedPunctuation.contains(c)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "PipelineBuilder{" + name + ", " + steps.size() + " step(s)}";
    }
}
