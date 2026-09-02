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
import java.util.List;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.api.TopologyPlan;
import org.acemq.amqp.transport.QueueCheck;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Transport;
import org.acemq.amqp.transport.TransportConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Works out what a {@link Topology} would change before changing it.
 *
 * <p>Declaring a topology blindly at start-up is the normal approach and it has a specific
 * failure mode: AMQP forbids changing most queue arguments in place, so a queue that already
 * exists with different settings does not quietly adapt. The declare fails, and on AMQP 0-9-1
 * it takes the channel with it. The failure therefore happens in production, at start-up,
 * rather than in review, where the difference was visible all along.
 *
 * <p>Planning first turns that into something readable. The plan says what exists and what
 * would be created; {@link ApplyMode#DRY_RUN} prints it without touching anything, and
 * {@link ApplyMode#VALIDATE} refuses to start when the topology is not already there.
 *
 * <p>A queue that exists with settings the topology disagrees with is reported as
 * {@link TopologyPlan.Kind#DRIFT} rather than as present, and {@link #apply} refuses to run
 * rather than failing partway through. AMQP has no way to read a queue's arguments back, so the
 * check is made by offering the declaration to the broker on a channel of its own and reading
 * its refusal — which names the argument and both values, and is therefore better than an
 * inspection API would have been.
 *
 * <p>What is still missing is the shadow-queue migration that <em>resolves</em> drift: creating
 * the replacement, moving the messages, and swapping the bindings. That is deliberately absent
 * rather than approximated, because the safe order depends on whether the queue can be drained
 * first, which is not something a library can decide.
 */
public final class TopologyPlanner {

    private static final Logger log = LoggerFactory.getLogger(TopologyPlanner.class);

    private final TransportConnection connection;
    private final Transport transport;

    TopologyPlanner(TransportConnection connection, Transport transport) {
        this.connection = connection;
        this.transport = transport;
    }

    /**
     * Works out what applying a topology would do, without doing any of it.
     *
     * @param topology what should exist
     * @return the plan
     */
    public TopologyPlan plan(Topology topology) {
        List<TopologyPlan.Action> actions = new ArrayList<>();

        // Exchanges are not inspected: AMQP offers no way to ask about one without a passive
        // declare that fails the channel when absent, and unlike a queue an exchange holds no
        // messages, so redeclaring an equivalent one is harmless. They are reported as
        // creations because that is the honest description of what apply will attempt.
        for (Topology.ExchangeSpec exchange : topology.exchanges()) {
            actions.add(action(TopologyPlan.Kind.CREATE, "exchange " + exchange.name() + " (" + exchange.type() + ")"));
        }

        for (Topology.QueueSpec queue : topology.queues()) {
            String described = "queue " + queue.name() + (queue.quorum() ? " (quorum)" : " (classic)");
            QueueType type = queue.quorum() ? QueueType.QUORUM : QueueType.CLASSIC;
            QueueCheck check = connection.checkQueue(queue.name(), type, queue.durable(), queue.arguments());

            switch (check.result()) {
                case ABSENT :
                    actions.add(action(TopologyPlan.Kind.CREATE, described));
                    break;
                case MATCHES :
                    actions.add(action(TopologyPlan.Kind.PRESENT, described));
                    break;
                case DIFFERS :
                    actions.add(action(TopologyPlan.Kind.DRIFT, described + " — " + check.detail()));
                    break;
                default :
                    // The transport cannot inspect the queue, so fall back to the smaller
                    // question it can answer. A queue reported as present here may still be
                    // wrong, which is why the plan says UNKNOWN rather than PRESENT.
                    actions.add(action(
                            connection.queueExists(queue.name())
                                    ? TopologyPlan.Kind.UNKNOWN
                                    : TopologyPlan.Kind.CREATE,
                            described));
                    break;
            }
        }

        for (Topology.BindingSpec binding : topology.bindings()) {
            actions.add(action(
                    TopologyPlan.Kind.CREATE,
                    "binding " + binding.queue() + " <- " + binding.exchange() + " [" + binding.routingKey() + "]"));
        }

        return TopologyPlan.of(actions);
    }

    /**
     * Applies a topology in the requested mode.
     *
     * @param topology what should exist
     * @param mode how much the apply is allowed to do
     * @return the plan that was computed, whether or not it was carried out
     * @throws AceMqException in {@link ApplyMode#VALIDATE} when something is missing, or when
     *     a quorum queue is requested from a broker that cannot provide one
     */
    public TopologyPlan apply(Topology topology, ApplyMode mode) {
        TopologyPlan plan = plan(topology);

        if (mode == ApplyMode.DRY_RUN) {
            log.info("topology dry run, nothing was changed:\n{}", plan.render());
            return plan;
        }

        // Before anything else, and in every mode that touches the broker. Declaring a drifted
        // queue fails at the broker and takes the channel with it, so the difference this makes
        // is between a message naming the argument and a channel-level protocol error partway
        // through applying a topology.
        if (plan.hasDrift()) {
            List<String> drifted = new ArrayList<>();
            for (TopologyPlan.Action action : plan.drift()) {
                drifted.add(action.description());
            }
            throw new AceMqException("the broker holds queues that do not match this topology, and AMQP does"
                    + " not allow changing them in place. Declaring them would fail and close the channel."
                    + " Either change the topology to match what is there, or migrate the queue: "
                    + String.join("; ", drifted));
        }

        if (mode == ApplyMode.VALIDATE) {
            List<TopologyPlan.Action> missing = plan.changes();
            // Exchanges and bindings always report as creations, so validation is only
            // meaningful for queues, which are the part that holds messages and the part an
            // operator provisions separately.
            List<String> missingQueues = new ArrayList<>();
            for (TopologyPlan.Action action : missing) {
                if (action.description().startsWith("queue ")) {
                    missingQueues.add(action.description());
                }
            }
            if (!missingQueues.isEmpty()) {
                throw new AceMqException("the topology is not present and this application is running in "
                        + "validate mode, so it will not create it. Missing: " + String.join(", ", missingQueues));
            }
            log.info("topology validated; every queue it needs is present");
            return plan;
        }

        for (Topology.ExchangeSpec exchange : topology.exchanges()) {
            connection.declareExchange(exchange.name(), exchange.type(), exchange.durable());
        }
        for (Topology.QueueSpec queue : topology.queues()) {
            QueueType type = queue.quorum() ? QueueType.QUORUM : QueueType.CLASSIC;
            if (type == QueueType.QUORUM && !transport.capabilities().contains(Capability.QUORUM_QUEUES)) {
                throw new AceMqException("the " + transport.name() + " broker does not support quorum queues, so '"
                        + queue.name() + "' cannot be declared as one. Declare it as a classic queue if that is"
                        + " acceptable for this queue.");
            }
            connection.declareQueue(queue.name(), type, queue.durable(), queue.arguments());
        }
        for (Topology.BindingSpec binding : topology.bindings()) {
            connection.bindQueue(binding.queue(), binding.exchange(), binding.routingKey());
        }

        log.info("topology applied: {}", plan);
        return plan;
    }

    private static TopologyPlan.Action action(TopologyPlan.Kind kind, String description) {
        return TopologyPlan.Action.of(kind, description);
    }
}
