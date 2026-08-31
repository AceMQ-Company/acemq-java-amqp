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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real RabbitMQ cluster of any size, for tests that need failure rather than function.
 *
 * <p>A single node proves that messages flow. It cannot prove what happens when the node
 * holding a queue leader disappears, which is the question quorum queues exist to answer. This
 * harness starts several brokers on a shared network, joins them into one cluster, and lets a
 * test stop nodes while traffic is running.
 *
 * <p>Testcontainers has no clustering support for RabbitMQ, so the join is performed the same
 * way an operator would: every node shares an Erlang cookie, then each one after the first
 * stops its application, joins the first, and starts again.
 *
 * <p>Clusters are expensive. Three nodes belong in the ordinary build; five and nine are
 * driven from Docker Compose in the nightly job, because nine brokers plus their queues do not
 * fit comfortably in a per-test lifecycle on a hosted runner.
 */
final class RabbitMqCluster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqCluster.class);
    private static final DockerImageName IMAGE = BrokerImage.current();
    private static final String COOKIE = "acemq-test-cookie";

    private final Network network;
    private final List<GenericContainer<?>> nodes = new ArrayList<>();
    private final int size;

    private RabbitMqCluster(int size) {
        this.size = size;
        this.network = Network.newNetwork();
    }

    /**
     * Starts a cluster and waits until every node has joined.
     *
     * @param size number of nodes
     * @return a running cluster
     */
    static RabbitMqCluster ofSize(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("a cluster needs at least one node, was " + size);
        }
        RabbitMqCluster cluster = new RabbitMqCluster(size);
        cluster.start();
        return cluster;
    }

    private void start() {
        for (int i = 1; i <= size; i++) {
            String hostname = "rabbit" + i;
            GenericContainer<?> node = new GenericContainer<>(IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases(hostname)
                    .withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname))
                    .withEnv("RABBITMQ_ERLANG_COOKIE", COOKIE)
                    // Nodes must address each other by a name that resolves inside the network,
                    // so the node name is pinned rather than left to the container id.
                    .withEnv("RABBITMQ_NODENAME", "rabbit@" + hostname)
                    .withExposedPorts(5672, 15672)
                    .withLogConsumer(new Slf4jLogConsumer(log).withPrefix(hostname))
                    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(3)));
            node.start();
            nodes.add(node);
        }

        for (int i = 1; i < nodes.size(); i++) {
            join(nodes.get(i), "rabbit@rabbit1");
        }
        log.info("cluster of {} node(s) is ready", size);
    }

    private void join(GenericContainer<?> node, String seed) {
        exec(node, "rabbitmqctl", "stop_app");
        exec(node, "rabbitmqctl", "join_cluster", seed);
        exec(node, "rabbitmqctl", "start_app");
        exec(node, "rabbitmqctl", "await_startup");
    }

    private static String exec(GenericContainer<?> node, String... command) {
        try {
            org.testcontainers.containers.Container.ExecResult result = node.execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(
                        "command " + String.join(" ", command) + " failed with exit code " + result.getExitCode()
                                + ": " + result.getStderr());
            }
            return result.getStdout();
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("could not run " + String.join(" ", command), e);
        }
    }

    /**
     * @param index zero-based node index
     * @return an AMQP URL pointing at that node
     */
    String amqpUrl(int index) {
        GenericContainer<?> node = nodes.get(index);
        return "amqp://guest:guest@" + node.getHost() + ":" + node.getMappedPort(5672);
    }

    /** @return an AMQP URL for the first node */
    String amqpUrl() {
        return amqpUrl(0);
    }

    int size() {
        return nodes.size();
    }

    /**
     * Reports how many replicas a quorum queue actually has.
     *
     * <p>The number matters as much as the cluster size. Replicating a queue to every node in a
     * large cluster costs a round trip per replica on every confirm and buys no extra safety
     * beyond a majority, so a sane default is a property worth asserting rather than assuming.
     *
     * @param queue queue name
     * @return the replica count reported by the broker
     */
    int replicaCount(String queue) {
        // rabbitmqctl renders the members of a quorum queue one per line.
        String output = exec(
                nodes.get(0), "rabbitmqctl", "list_queues", "--quiet", "--formatter", "json", "name", "members");
        return countMembers(output, queue);
    }

    /** Extracts the member count for one queue from rabbitmqctl's JSON output. */
    static int countMembers(String json, String queue) {
        int at = json.indexOf("\"name\":\"" + queue + "\"");
        if (at < 0) {
            throw new IllegalStateException("queue '" + queue + "' not found in: " + json);
        }
        int membersAt = json.indexOf("\"members\":", at);
        if (membersAt < 0) {
            return 0;
        }
        int open = json.indexOf('[', membersAt);
        int close = json.indexOf(']', open);
        String members = json.substring(open + 1, close).trim();
        if (members.isEmpty()) {
            return 0;
        }
        return members.split(",").length;
    }

    /**
     * Stops one node, as an operator or an outage would.
     *
     * @param index zero-based node index
     */
    void stopNode(int index) {
        GenericContainer<?> node = nodes.get(index);
        log.info("stopping node {}", index + 1);
        node.stop();
    }

    /**
     * Finds the node currently leading a quorum queue.
     *
     * @param queue queue name
     * @return zero-based index of the leader, or -1 when it cannot be determined
     */
    int leaderOf(String queue) {
        String output = exec(nodes.get(0), "rabbitmqctl", "list_queues", "--quiet", "--formatter", "json", "name",
                "leader");
        int at = output.indexOf("\"name\":\"" + queue + "\"");
        if (at < 0) {
            return -1;
        }
        int leaderAt = output.indexOf("\"leader\":\"", at);
        if (leaderAt < 0) {
            return -1;
        }
        int start = leaderAt + "\"leader\":\"".length();
        String leader = output.substring(start, output.indexOf('"', start));
        for (int i = 0; i < nodes.size(); i++) {
            if (leader.equals("rabbit@rabbit" + (i + 1))) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void close() {
        for (GenericContainer<?> node : nodes) {
            try {
                node.stop();
            } catch (RuntimeException e) {
                log.debug("ignoring error while stopping a node", e);
            }
        }
        nodes.clear();
        network.close();
    }
}
