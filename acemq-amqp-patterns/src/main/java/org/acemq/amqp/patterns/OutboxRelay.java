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
package org.acemq.amqp.patterns;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.OutboxStore;
import org.acemq.amqp.api.Publisher;
import org.acemq.amqp.core.AceMq;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves messages out of the outbox and into the broker.
 *
 * <p>The half of the pattern that does the sending. It claims a batch, publishes each record and
 * marks it, and it does so on its own thread so that no request ever waits for a broker: the
 * point of writing to a table was to let the request commit and return, and a relay that ran
 * inline would give that back.
 *
 * <p>The order of the two steps is the only interesting decision, and it is settled by which
 * failure is survivable. Publishing before marking means a relay that dies in between will
 * publish the message again on restart. Marking before publishing means the same crash loses the
 * message entirely. A duplicate can be absorbed by an idempotent consumer; a message that was
 * never sent and is recorded as sent cannot be recovered by anyone. So: publish, then mark, and
 * accept at-least-once as the guarantee on offer.
 *
 * <p>A failed publish releases the claim and increments the attempt count rather than stopping
 * the batch, because one unroutable message must not hold up the queue behind it. After enough
 * failures the store stops handing the record out and it stays in the table, visible, for someone
 * to look at.
 *
 * <p>Ordering between messages is preserved only while a single relay runs: records are claimed
 * oldest first and published one at a time. Run two relays for throughput and you have traded
 * ordering for it, which is usually the right trade and never a silent one.
 */
public final class OutboxRelay implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_LEASE = Duration.ofMinutes(1);
    private static final int MAX_BATCHES_PER_RUN = 10;

    private final AceMq mq;
    private final OutboxStore store;
    private final int batchSize;
    private final Duration pollInterval;
    private final Duration lease;

    private final Map<String, Publisher<String>> publishers = new ConcurrentHashMap<>();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile @Nullable ScheduledExecutorService scheduler;

    /**
     * @param mq connection to publish through
     * @param store outbox to drain
     */
    public OutboxRelay(AceMq mq, OutboxStore store) {
        this(mq, store, DEFAULT_BATCH_SIZE, DEFAULT_POLL_INTERVAL, DEFAULT_LEASE);
    }

    /**
     * @param mq connection to publish through
     * @param store outbox to drain
     * @param batchSize how many records to claim at once
     * @param pollInterval how long to wait between passes when there is nothing to do
     * @param lease how long a claim holds; must comfortably exceed the time a batch takes to
     *     publish, or another relay will start sending the same messages alongside this one
     */
    public OutboxRelay(AceMq mq, OutboxStore store, int batchSize, Duration pollInterval, Duration lease) {
        this.mq = Objects.requireNonNull(mq, "mq");
        this.store = Objects.requireNonNull(store, "store");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1, was " + batchSize);
        }
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive, was " + pollInterval);
        }
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("lease must be positive, was " + lease);
        }
        this.batchSize = batchSize;
        this.pollInterval = pollInterval;
        this.lease = lease;
    }

    /**
     * Starts draining the outbox on a background thread.
     *
     * <p>Calling this twice does nothing the second time.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "acemq-outbox-relay");
            // A daemon thread, so an application that forgets to close the relay can still exit.
            thread.setDaemon(true);
            return thread;
        });
        this.scheduler = executor;
        long period = Math.max(1L, pollInterval.toMillis());
        executor.scheduleWithFixedDelay(this::runSafely, 0L, period, TimeUnit.MILLISECONDS);
        log.info("outbox relay started, polling every {}", pollInterval);
    }

    private void runSafely() {
        // scheduleWithFixedDelay cancels the task for good if it ever throws, and does so without
        // a word: the relay would simply stop, the table would fill, and nothing would say why.
        // Catching Throwable here is the difference between a bad pass and a dead relay.
        try {
            drain();
        } catch (Throwable failure) {
            log.error("outbox relay pass failed; the next pass will retry", failure);
        }
    }

    /**
     * Publishes everything currently waiting, in batches.
     *
     * <p>Stops after a fixed number of batches even if more remain, so that a large backlog is
     * worked through steadily instead of monopolising the thread and starving the poll that
     * measures progress.
     *
     * @return how many records were published
     */
    public int drain() {
        int total = 0;
        for (int pass = 0; pass < MAX_BATCHES_PER_RUN; pass++) {
            int sent = drainOnce();
            total += sent;
            if (sent < batchSize) {
                break;
            }
        }
        return total;
    }

    /**
     * Claims one batch and publishes it.
     *
     * @return how many records were published; zero when the outbox is empty
     */
    public int drainOnce() {
        List<OutboxRecord> batch = store.claimBatch(batchSize, lease);
        if (batch.isEmpty()) {
            return 0;
        }

        int sent = 0;
        for (OutboxRecord record : batch) {
            if (publish(record)) {
                sent++;
            }
        }
        log.debug("outbox relay published {} of {} claimed records", sent, batch.size());
        return sent;
    }

    private boolean publish(OutboxRecord record) {
        try {
            publisherFor(record.exchange(), record.routingKey()).send(record.payload(), record.envelope());
        } catch (RuntimeException failure) {
            failed.incrementAndGet();
            log.warn("outbox record {} could not be published: {}", record.id(), failure.toString());
            store.markFailed(record.id(), failure.toString());
            return false;
        }

        try {
            store.markPublished(record.id());
        } catch (RuntimeException failure) {
            // The message is already at the broker. Failing to record that is survivable — the
            // record will be claimed again and published a second time — so it is logged rather
            // than rethrown, and the rest of the batch still goes out.
            log.warn("outbox record {} was published but could not be marked; it will be published again: {}",
                    record.id(), failure.toString());
        }
        published.incrementAndGet();
        return true;
    }

    private Publisher<String> publisherFor(String exchange, String routingKey) {
        // Publishers are meant to be long lived, and building one per record would create a
        // channel's worth of work for every message.
        //
        // VERBATIM is what makes an outbox message readable by a typed consumer. The
        // payload was serialised inside the caller's transaction, which is the whole
        // reason the pattern works, so the relay's job is to put those bytes on the wire
        // unchanged. Publishing them through the ordinary codec encodes them a second
        // time, and what arrives is a JSON string containing JSON: a consumer asking for
        // the event type fails with "no String-argument constructor", and the only thing
        // able to read the queue is one taking String and parsing it by hand.
        return publishers.computeIfAbsent(
                exchange + ' ' + routingKey,
                key -> mq.publisher(exchange, routingKey, String.class).as(VERBATIM));
    }

    /**
     * Writes an already-serialised payload out as it is.
     *
     * <p>Reports {@code application/json} because that is what an outbox holds by
     * convention and what a consumer's codec will be asked to read. It never decodes:
     * nothing consumes through this codec, and a relay able to read its own messages
     * would be doing something it has no business doing.
     */
    private static final Codec VERBATIM = new Codec() {

        @Override
        public String contentType() {
            return "application/json";
        }

        @Override
        public byte[] encode(Object payload) {
            return String.valueOf(payload).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public <T> T decode(byte[] body, Class<T> target) {
            throw new UnsupportedOperationException("the outbox relay only publishes");
        }

        @Override
        public boolean canDecode(@Nullable String contentType) {
            return false;
        }

        @Override
        public String toString() {
            return "OutboxRelay.VERBATIM";
        }
    };

    /** @return how many records this relay has published */
    public long published() {
        return published.get();
    }

    /** @return how many publish attempts this relay has seen fail */
    public long failed() {
        return failed.get();
    }

    /** @return whether the background thread is running */
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() {
        running.set(false);
        ScheduledExecutorService executor = this.scheduler;
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("the outbox relay thread did not stop within five seconds");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.scheduler = null;
        }
        publishers.values().forEach(Publisher::close);
        publishers.clear();
    }

    @Override
    public String toString() {
        return "OutboxRelay{batchSize=" + batchSize + ", pollInterval=" + pollInterval + ", published="
                + published.get() + ", failed=" + failed.get() + "}";
    }
}
