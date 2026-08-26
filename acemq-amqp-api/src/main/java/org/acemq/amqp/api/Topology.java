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
package org.acemq.amqp.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A topology as data: what exchanges, queues and bindings should exist.
 *
 * <p>Declared rather than performed. Because it is a value, it can be printed, diffed,
 * reviewed in a pull request and applied by something other than the application that
 * needs it, which is the difference between a topology you can reason about and a
 * sequence of declare calls scattered through start-up code.
 */
public final class Topology {

    private final List<ExchangeSpec> exchanges;
    private final List<QueueSpec> queues;
    private final List<BindingSpec> bindings;

    private Topology(Builder builder) {
        this.exchanges = Collections.unmodifiableList(new ArrayList<>(builder.exchanges));
        this.queues = Collections.unmodifiableList(new ArrayList<>(builder.queues));
        this.bindings = Collections.unmodifiableList(new ArrayList<>(builder.bindings));
    }

    public static Builder define() {
        return new Builder();
    }

    public List<ExchangeSpec> exchanges() {
        return exchanges;
    }

    public List<QueueSpec> queues() {
        return queues;
    }

    public List<BindingSpec> bindings() {
        return bindings;
    }

    @Override
    public String toString() {
        return "Topology{exchanges=" + exchanges.size() + ", queues=" + queues.size() + ", bindings="
                + bindings.size() + "}";
    }

    /** An exchange that should exist. */
    public static final class ExchangeSpec {

        private final String name;
        private final String type;
        private final boolean durable;

        ExchangeSpec(String name, String type, boolean durable) {
            this.name = Objects.requireNonNull(name, "exchange name must not be null");
            this.type = Objects.requireNonNull(type, "exchange type must not be null");
            this.durable = durable;
        }

        public String name() {
            return name;
        }

        public String type() {
            return type;
        }

        public boolean durable() {
            return durable;
        }
    }

    /** A queue that should exist. */
    public static final class QueueSpec {

        private final String name;
        private final boolean quorum;
        private final boolean durable;
        private final Map<String, Object> arguments;

        QueueSpec(String name, boolean quorum, boolean durable, Map<String, Object> arguments) {
            this.name = Objects.requireNonNull(name, "queue name must not be null");
            this.quorum = quorum;
            this.durable = durable;
            this.arguments = Collections.unmodifiableMap(new LinkedHashMap<>(arguments));
        }

        public String name() {
            return name;
        }

        public boolean quorum() {
            return quorum;
        }

        public boolean durable() {
            return durable;
        }

        public Map<String, Object> arguments() {
            return arguments;
        }
    }

    /** A binding that should exist. */
    public static final class BindingSpec {

        private final String queue;
        private final String exchange;
        private final String routingKey;

        BindingSpec(String queue, String exchange, String routingKey) {
            this.queue = Objects.requireNonNull(queue, "queue must not be null");
            this.exchange = Objects.requireNonNull(exchange, "exchange must not be null");
            this.routingKey = routingKey == null ? "" : routingKey;
        }

        public String queue() {
            return queue;
        }

        public String exchange() {
            return exchange;
        }

        public String routingKey() {
            return routingKey;
        }
    }

    /** Builds a {@link Topology}. */
    public static final class Builder {

        private final List<ExchangeSpec> exchanges = new ArrayList<>();
        private final List<QueueSpec> queues = new ArrayList<>();
        private final List<BindingSpec> bindings = new ArrayList<>();

        /**
         * @param name exchange name
         * @param type {@code direct}, {@code topic}, {@code fanout} or {@code headers}
         * @return this builder
         */
        public Builder exchange(String name, String type) {
            exchanges.add(new ExchangeSpec(name, type, true));
            return this;
        }

        /**
         * A durable quorum queue, which is the default everywhere in this library.
         *
         * @param name queue name
         * @return this builder
         */
        public Builder queue(String name) {
            queues.add(new QueueSpec(name, true, true, Collections.emptyMap()));
            return this;
        }

        /**
         * @param name queue name
         * @param arguments broker-specific arguments
         * @return this builder
         */
        public Builder classicQueue(String name, Map<String, Object> arguments) {
            queues.add(new QueueSpec(name, false, true, arguments));
            return this;
        }

        /**
         * @param queue queue to bind
         * @param exchange exchange to bind it to
         * @param routingKey routing key or pattern
         * @return this builder
         */
        public Builder bind(String queue, String exchange, String routingKey) {
            bindings.add(new BindingSpec(queue, exchange, routingKey));
            return this;
        }

        public Topology build() {
            return new Topology(this);
        }
    }
}
