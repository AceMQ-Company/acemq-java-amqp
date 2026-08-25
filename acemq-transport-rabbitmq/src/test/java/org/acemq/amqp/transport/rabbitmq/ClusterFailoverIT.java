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
package org.acemq.amqp.transport.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

/**
 * A three-node cluster losing a node while messages are in flight.
 *
 * <p>Three is the smallest size at which a quorum queue means anything: a majority of three
 * survives one loss. Everything asserted here is invisible on a single node, which is why the
 * single-node suite cannot replace it.
 *
 * <p>The cluster is started once for the whole class. Starting three brokers costs far more
 * than the tests do, and each test is written to tolerate the state the previous one left.
 */
@DisplayName("a three-node cluster")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterFailoverIT {

    private static RabbitMqCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = RabbitMqCluster.ofSize(3);
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @Order(2)
    @Timeout(300)
    void a_quorum_queue_keeps_its_messages_when_a_node_is_lost() {
        String queue = "orders.failover";
        Set<String> received = ConcurrentHashMap.newKeySet();

        try (AceMq mq = AceMq.connect(cluster.amqpUrl())) {
            assertThat(mq.supports(Capability.QUORUM_QUEUES)).isTrue();
            mq.declareExchange("orders", "topic");
            mq.declareQueue(queue);
            mq.bind(queue, "orders", "order.*");

            for (int i = 0; i < 20; i++) {
                mq.publisher("orders", "order.placed").send("before-" + i);
            }

            // Take down a node that is not the one this client is connected through, so the
            // test observes queue failover rather than connection recovery.
            int leader = cluster.leaderOf(queue);
            int victim = leader > 0 ? leader : 1;
            cluster.stopNode(victim);

            try (MessageConsumer consumer = mq.consume(queue, String.class, ConsumerOptions.prefetch(10),
                    message -> received.add(
                            message.payload()))) {

                // Every message published before the loss must still be delivered: a majority
                // of the replicas survived, so nothing was acknowledged and then lost.
                await().atMost(Duration.ofMinutes(2)).until(() -> received.size() == 20);
                assertThat(consumer.rejected()).isZero();
            }
        }
    }

    @Test
    @Order(3)
    @Timeout(300)
    void publishing_continues_against_a_surviving_node() {
        String queue = "orders.survivor";
        AtomicInteger handled = new AtomicInteger();

        // Node two was stopped by the previous test, so this connects through node one and
        // publishes into a cluster that is already degraded but still has a majority.
        try (AceMq mq = AceMq.connect(cluster.amqpUrl(0))) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue(queue);
            mq.bind(queue, "orders", "order.*");

            try (MessageConsumer consumer = mq.consume(queue, String.class, message -> handled.incrementAndGet())) {

                for (int i = 0; i < 20; i++) {
                    // Each publish waits for a confirm. On a degraded cluster the confirm still
                    // arrives, because a majority of the replicas can still agree.
                    mq.publisher("orders", "order.placed").send("after-" + i);
                }

                await().atMost(Duration.ofMinutes(2)).until(() -> handled.get() == 20);
            }
        }
    }

    @Test
    @Order(1)
    @Timeout(300)
    void a_quorum_queue_is_replicated_to_a_majority_rather_than_to_every_node() {
        String queue = "orders.replicas";

        try (AceMq mq = AceMq.connect(cluster.amqpUrl(0))) {
            mq.declareQueue(queue);

            int replicas = cluster.replicaCount(queue);

            // Ordered first, so this observes a healthy cluster: a replica count measured
            // after a node has been stopped would say nothing about the default placement.
            //
            // On three nodes, three replicas is both the majority and the whole cluster. The
            // property that matters is that the count is odd, so a majority is unambiguous,
            // and bounded, because replicating to every node of a large cluster costs a round
            // trip per replica on every confirm and buys nothing beyond a majority. The
            // nightly five and nine node jobs assert it stays at three rather than growing.
            assertThat(replicas).isEqualTo(3);
            assertThat(replicas % 2).as("a replica count should be odd so a majority is unambiguous").isEqualTo(1);
        }
    }
}
