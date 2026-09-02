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

import java.util.Optional;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Step;
import org.jspecify.annotations.Nullable;

/**
 * One step of a pipeline, with how it consumes and what it publishes.
 *
 * <p>Each step carries its own consumer options, because steps fail differently: validation
 * fails permanently and should not be retried, while enrichment fails because an HTTP call timed
 * out and should be.
 *
 * @param <I> payload this step receives
 * @param <O> payload it produces
 */
final class PipelineStep<I, O> {

    private final String name;
    private final Class<I> inputType;
    private final Class<O> outputType;
    private final Step<I, O> handler;
    private final ConsumerOptions options;
    private final int concurrency;
    private final @Nullable Codec codec;

    /**
     * What this step is for, in a sentence, or null when nobody said.
     *
     * <p>Separate from the name because the name cannot carry it: a step name is a routing key
     * and a routing slip entry, so it is constrained to letters, digits, dashes and dots. The
     * description has no such job and is free text, which is what makes it useful in a log line
     * that somebody is reading at three in the morning.
     */
    private final @Nullable String description;

    PipelineStep(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            Step<I, O> handler,
            ConsumerOptions options,
            int concurrency,
            @Nullable Codec codec) {
        this(name, inputType, outputType, handler, options, concurrency, codec, null);
    }

    PipelineStep(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            Step<I, O> handler,
            ConsumerOptions options,
            int concurrency,
            @Nullable Codec codec,
            @Nullable String description) {
        this.description = description;
        this.name = name;
        this.inputType = inputType;
        this.outputType = outputType;
        this.handler = handler;
        this.options = options;
        this.concurrency = concurrency;
        this.codec = codec;
    }

    String name() {
        return name;
    }

    /** @return what this step is for, when somebody said */
    Optional<String> description() {
        return Optional.ofNullable(description);
    }

    /**
     * @return the description when there is one, otherwise the name. What a log line should
     *     print: a step is always identifiable, and better identified when it was described
     */
    String label() {
        return description == null ? name : name + " (" + description + ")";
    }

    PipelineStep<I, O> describedAs(String text) {
        return new PipelineStep<>(name, inputType, outputType, handler, options, concurrency, codec, text);
    }

    Class<I> inputType() {
        return inputType;
    }

    /** @return what this step produces, which is what the next one receives */
    Class<O> outputType() {
        return outputType;
    }

    Step<I, O> handler() {
        return handler;
    }

    ConsumerOptions options() {
        return options;
    }

    int concurrency() {
        return concurrency;
    }

    /**
     * @return the format this step's <em>output</em> is published in, when it differs from the
     *     connection's. A format change is how the next hop is encoded rather than a step of its
     *     own: making it a step would add a queue, a publish and a consumer whose whole job is
     *     to deserialize and reserialize the same object
     */
    Optional<Codec> codec() {
        return Optional.ofNullable(codec);
    }

    PipelineStep<I, O> with(ConsumerOptions updated, int howMany, @Nullable Codec encoding) {
        return new PipelineStep<>(name, inputType, outputType, handler, updated, howMany, encoding, description);
    }

    @Override
    public String toString() {
        return "PipelineStep{" + label() + " <- " + inputType.getSimpleName() + "}";
    }
}
