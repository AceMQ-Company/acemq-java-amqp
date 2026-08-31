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

    PipelineStep(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            Step<I, O> handler,
            ConsumerOptions options,
            int concurrency,
            @Nullable Codec codec) {
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
        return new PipelineStep<>(name, inputType, outputType, handler, updated, howMany, encoding);
    }

    @Override
    public String toString() {
        return "PipelineStep{" + name + " <- " + inputType.getSimpleName() + "}";
    }
}
