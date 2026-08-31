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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionBlockedException;
import org.acemq.amqp.transport.ConnectionConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * A real broker under a real resource alarm.
 *
 * <p>The in-memory tests describe what the library should do; this one proves the broker does what
 * they assume. Setting the memory high watermark to zero puts RabbitMQ into the same alarm state a
 * production node reaches under load: it stops reading from publishing sockets and sends
 * {@code connection.blocked}. Nothing about it is simulated.
 *
 * <p>These tests are also what established the timing that the implementation is built around.
 * RabbitMQ does not tell an idle connection that an alarm has begun — it tells a connection when
 * that connection next publishes. So the block is discovered while waiting for the first
 * message's confirm, not before sending it, and only later messages are refused up front. An
 * implementation that only checked before publishing would still hang on the first message, which
 * is the one that matters.
 */
@Testcontainers
@DisplayName("publishing to a broker under a memory alarm")
class BlockedConnectionIT {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    private AceMq mq;

    @AfterEach
    void disconnect() throws Exception {
        // First, so a failed test cannot leave the broker in alarm for whatever runs next.
        clearAlarm();
        if (mq != null) {
            mq.deleteQueue("orders.new");
            mq.close();
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a publish waits for the alarm to clear, then goes through")
    void publishWaitsForTheAlarmToClear() throws Exception {
        connect(Duration.ofSeconds(90));
        raiseAlarm();

        CountDownLatch published = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread publisher = new Thread(() -> {
            try {
                mq.publisher("orders", "order.created", String.class).send("one");
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                published.countDown();
            }
        }, "blocked-publisher");
        publisher.start();

        // The block becomes visible because of that publish, not because of the alarm.
        await().atMost(Duration.ofSeconds(20)).until(mq::isBlocked);
        assertThat(mq.blockedReason()).isPresent();
        assertThat(published.await(2, TimeUnit.SECONDS))
                .as("the publish should still be waiting while the broker is in alarm")
                .isFalse();

        clearAlarm();

        assertThat(published.await(30, TimeUnit.SECONDS))
                .as("the publish should complete once the alarm clears")
                .isTrue();
        assertThat(failure.get()).isNull();
        await().atMost(Duration.ofSeconds(20)).until(() -> !mq.isBlocked());
        publisher.join(TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    @Timeout(120)
    @DisplayName("a publish gives up when the alarm outlasts the timeout, and names the alarm")
    void publishGivesUpWhenTheAlarmOutlastsTheTimeout() throws Exception {
        connect(Duration.ofSeconds(5));
        raiseAlarm();

        long startedAt = System.nanoTime();
        Throwable thrown = catchPublish("one");

        assertThat(thrown)
                .as("without this, the alarm is reported as a missing confirm, which blames the wrong system")
                .isInstanceOf(ConnectionBlockedException.class);
        ConnectionBlockedException blocked = (ConnectionBlockedException) thrown;
        // RabbitMQ's own wording. Asserting it rather than our paraphrase is what proves the
        // reason survived from the broker to the caller instead of being invented here.
        assertThat(blocked.reason()).contains("memory");
        assertThat(blocked.mayHaveBeenPublished())
                .as("this message was already on the wire when the block was discovered")
                .isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .as("it should wait for the configured timeout, not fail immediately")
                .isGreaterThanOrEqualTo(Duration.ofSeconds(4));

        // The next one is refused before anything is written, because by now the block is known.
        Throwable second = catchPublish("two");
        assertThat(second).isInstanceOf(ConnectionBlockedException.class);
        assertThat(((ConnectionBlockedException) second).mayHaveBeenPublished())
                .as("nothing was written for this one, so it is safe to resend")
                .isFalse();
    }

    private void connect(Duration blockedTimeout) {
        mq = AceMq.connect(ConnectionConfig.url(BROKER.getAmqpUrl())
                .blockedTimeout(blockedTimeout)
                .build());
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new");
        mq.bind("orders.new", "orders", "order.*");
    }

    private Throwable catchPublish(String payload) {
        try {
            mq.publisher("orders", "order.created", String.class).send(payload);
            return null;
        } catch (Throwable t) {
            return t;
        }
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
            throw new IllegalStateException(
                    "could not run " + String.join(" ", command) + ": " + result.getStderr());
        }
    }
}
