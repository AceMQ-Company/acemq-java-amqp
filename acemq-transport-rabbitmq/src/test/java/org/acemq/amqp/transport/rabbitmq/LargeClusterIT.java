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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The five and nine node clusters, run nightly against a Docker Compose stack.
 *
 * <p>These are not started by the test. Nine brokers take minutes to come up and would make
 * the ordinary build unusable, so the nightly job brings the stack up with
 * {@code compose/cluster-<size>.yml}, joins it with {@code compose/join-cluster.sh}, and then
 * runs this class with {@code -Dacemq.cluster.size=<size>}. Without that property the class is
 * skipped, which is why an ordinary {@code mvn verify} never notices it.
 *
 * <p>The question worth asking at this size is not whether messages flow — three nodes already
 * answered that — but whether anything grows with the cluster that should not. A queue
 * replicated to all nine nodes would still work, and would quietly cost a round trip per
 * replica on every confirm.
 */
@DisplayName("a five or nine node cluster")
@EnabledIfSystemProperty(named = "acemq.cluster.size", matches = "\\d+", disabledReason = "started by the nightly job from compose/cluster-<size>.yml")
class LargeClusterIT {

    private static int clusterSize;

    @BeforeAll
    static void readClusterSize() {
        clusterSize = Integer.parseInt(System.getProperty("acemq.cluster.size", "5"));
    }

    /** @return the URL of the first node, published by the compose stack on 5671 */
    private static String url() {
        return "amqp://guest:guest@localhost:5671";
    }

    @Test
    @Timeout(600)
    void a_quorum_queue_is_replicated_to_three_nodes_rather_than_to_all_of_them() {
        String queue = "orders.large." + clusterSize;

        try (AceMq mq = AceMq.connect(url())) {
            mq.declareQueue(queue);

            int replicas = replicaCount(queue);

            // This is the assertion the whole nightly job exists for. On nine nodes a queue
            // replicated everywhere still functions, so no functional test would catch it;
            // the cost shows up only as latency under load. Three replicas tolerate one
            // failure, five tolerate two, and neither needs the other six nodes involved.
            assertThat(replicas)
                    .as("a quorum queue on a %d node cluster should not replicate to every node", clusterSize)
                    .isLessThanOrEqualTo(5);
            assertThat(replicas).isEqualTo(3);
        }
    }

    @Test
    @Timeout(600)
    void messages_survive_the_loss_of_a_minority_of_replicas() {
        String queue = "orders.large.failover." + clusterSize;
        Set<String> received = ConcurrentHashMap.newKeySet();

        try (AceMq mq = AceMq.connect(url())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue(queue);
            mq.bind(queue, "orders", "order.*");

            for (int i = 0; i < 50; i++) {
                mq.publisher("orders", "order.placed").send("message-" + i);
            }

            // Stop a node that is not the one this client is connected through. With three
            // replicas spread across a larger cluster, losing one leaves a majority.
            docker("stop", "acemq-rabbit3");

            try (MessageConsumer consumer = mq.consume(queue, String.class, ConsumerOptions.prefetch(20),
                    message -> received.add(
                            message.payload()))) {
                await().atMost(Duration.ofMinutes(5)).until(() -> received.size() == 50);
            }

            docker("start", "acemq-rabbit3");
        }
    }

    /** Counts the members of a quorum queue by asking node one. */
    private static int replicaCount(String queue) {
        String output = docker(
                "exec", "acemq-rabbit1", "rabbitmqctl", "list_queues", "--quiet", "--formatter", "json", "name",
                "members");
        return RabbitMqCluster.countMembers(output, queue);
    }

    /** Runs a docker command against the compose stack. */
    private static String docker(String... arguments) {
        String[] command = new String[arguments.length + 1];
        command[0] = "docker";
        System.arraycopy(arguments, 0, command, 1, arguments.length);
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line);
                }
            }
            process.waitFor();
            return output.toString();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("could not run " + String.join(" ", command), e);
        }
    }
}
