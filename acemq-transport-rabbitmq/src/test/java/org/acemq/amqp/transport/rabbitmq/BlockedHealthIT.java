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

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Reading the health facts off a connection the broker has genuinely blocked.
 *
 * <p>Sibling libraries reported a blocked broker correctly in principle and hung in practice,
 * because the health check first asked the broker a question. A blocked connection is one
 * RabbitMQ has stopped reading from, so the question never arrives and the careful
 * blocked-aware branch underneath it never runs. The bug is not in what the check concludes;
 * it is in getting far enough to conclude anything.
 *
 * <p>This test exists to keep that shape out of Java. It does two things, and the second is
 * what gives the first any meaning:
 *
 * <ol>
 *   <li>It times the five facts {@code AceMqHealthIndicator} reads — open, transport, blocked,
 *       the reason, and in-flight — against a connection under a real {@code connection.blocked},
 *       and requires them to answer effectively instantly.
 *   <li>It then shows, on the same connection, that a broker round trip does <em>not</em> answer.
 *       Without this, the first assertion would pass just as happily against a broker that was
 *       never blocked at all, and would prove nothing.
 * </ol>
 *
 * <p>The alarm is real: {@code set_vm_memory_high_watermark 0} puts the node into the state a
 * production broker reaches under memory pressure. Nothing here is simulated or stubbed.
 */
@Testcontainers
@DisplayName("checking health on a connection under a memory alarm")
class BlockedHealthIT {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(BrokerImage.current());

    /**
     * Long enough that the publisher that provokes the block stays parked for the whole test.
     * The block has to be discovered by a publish, and it has to still be in force while the
     * health facts are read.
     */
    private static final Duration BLOCKED_TIMEOUT = Duration.ofSeconds(90);

    private AceMq mq;

    @AfterEach
    void disconnect() throws Exception {
        // First, so a failed test cannot leave the broker in alarm for whatever runs next, and
        // so that closing does not wait on a broker that has stopped reading.
        clearAlarm();
        if (mq != null) {
            mq.deleteQueue("orders.new");
            mq.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("the health facts answer at once, while a round trip on the same connection does not")
    void healthFactsAnswerWhileTheConnectionIsBlocked() throws Exception {
        connect();
        raiseAlarm();
        CountDownLatch published = provokeTheBlockByPublishing();

        await().atMost(Duration.ofSeconds(30)).until(mq::isBlocked);
        assertThat(published.await(1, TimeUnit.SECONDS))
                .as("the publish should still be parked, which is what keeps the connection blocked")
                .isFalse();

        // Exactly what AceMqHealthIndicator.doHealthCheck reads, in the order it reads them.
        long startedAt = System.nanoTime();
        boolean open = mq.isOpen();
        String transport = mq.transportName();
        boolean blocked = mq.isBlocked();
        long inFlight = mq.inFlight();
        Optional<String> reason = mq.blockedReason();
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(open)
                .as("a blocked connection is open: the broker is talking to us, it is just not reading")
                .isTrue();
        assertThat(blocked).isTrue();
        assertThat(transport).isEqualTo("rabbitmq");
        assertThat(inFlight).isZero();
        // RabbitMQ's own wording, so this proves the reason survived from the broker rather than
        // being invented here. It is the detail an operator reads first.
        assertThat(reason).isPresent();
        assertThat(reason.orElseThrow()).contains("memory");
        assertThat(took)
                .as("health is answered from state the broker already pushed to us, so it costs no round trip")
                .isLessThan(Duration.ofMillis(100));

        assertARoundTripWouldHaveHung();
    }

    /**
     * The control. A passive queue declare is the cheapest question this library asks a broker,
     * and it is the shape every hung health check in the sibling libraries had. On a blocked
     * connection it does not come back, because the request is never read.
     */
    private void assertARoundTripWouldHaveHung() throws Exception {
        CountDownLatch answered = new CountDownLatch(1);
        Thread probe = new Thread(
                () -> {
                    try {
                        mq.messageCount("orders.new");
                    } catch (Throwable ignored) {
                        // How it ends does not matter; that it ends at all is the measurement.
                    } finally {
                        answered.countDown();
                    }
                },
                "would-be-health-probe");
        probe.setDaemon(true);
        probe.start();

        assertThat(answered.await(5, TimeUnit.SECONDS))
                .as("a health check built on this round trip would have hung here instead of reporting")
                .isFalse();

        // And it was the alarm holding it, not a broken broker: clearing the alarm releases it.
        clearAlarm();
        assertThat(answered.await(60, TimeUnit.SECONDS))
                .as("the same round trip completes once the broker starts reading again")
                .isTrue();
    }

    private CountDownLatch provokeTheBlockByPublishing() {
        // RabbitMQ tells a connection it is blocked when that connection next publishes, not when
        // the alarm begins. Without this publish there is no connection.blocked to react to.
        CountDownLatch published = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread publisher = new Thread(
                () -> {
                    try {
                        mq.publisher("orders", "order.created", String.class).send("one");
                    } catch (Throwable t) {
                        failure.set(t);
                    } finally {
                        published.countDown();
                    }
                },
                "blocked-publisher");
        publisher.setDaemon(true);
        publisher.start();
        return published;
    }

    private void connect() {
        mq = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl())
                .blockedTimeout(BLOCKED_TIMEOUT)
                .build());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new");
        mq.bind("orders.new", "orders", "order.*");
    }

    /** Puts the broker into a memory alarm by declaring that no memory at all is available. */
    private static void raiseAlarm() throws IOException, InterruptedException {
        execute("rabbitmqctl", "set_vm_memory_high_watermark", "0");
    }

    /** Restores the default watermark, which clears the alarm. */
    private static void clearAlarm() throws IOException, InterruptedException {
        execute("rabbitmqctl", "set_vm_memory_high_watermark", "0.4");
    }

    private static void execute(String... command) throws IOException, InterruptedException {
        Container.ExecResult result = BROKER.execInContainer(command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("could not run " + String.join(" ", command) + ": " + result.getStderr());
        }
    }
}
