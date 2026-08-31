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

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.PulledMessage;
import org.acemq.amqp.transport.TransportConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves messages out of a dead-letter queue or parking lot and back to the queue they failed in.
 *
 * <p>The half of dead-lettering that is usually missing. Capturing a failed message is easy and
 * most libraries stop there, which leaves an operator with a queue full of evidence and no way to
 * act on it — and a dead-letter queue nobody can drain is a slower way of losing data, not an
 * alternative to it. The messages are already safe; this is how they get another chance once the
 * bug is fixed or the downstream service is back.
 *
 * <pre>{@code
 * Replay replay = mq.replay("orders.new");
 *
 * replay.pending();              // 412 waiting in orders.new.dlq — look before touching
 * replay.replay(50);             // move the first 50 back
 * replay.replayAll();            // or all of them
 *
 * replay.parked().replayAll();   // the ones that could not even be decoded
 * }</pre>
 *
 * <h2>What it does to a message</h2>
 *
 * <p>The body is untouched. The attempt counter is reset, so a replayed message gets the full
 * retry ladder again rather than being dead-lettered immediately by the count that put it here.
 * Three headers are added: where it came from, when it was replayed, and how many times it has
 * been replayed before — the last one because a message on its fifth trip through a dead-letter
 * queue is telling you something that a fresh-looking message would not.
 *
 * <p>Messages go back to the <strong>queue</strong> they failed in, not through the exchange that
 * originally routed them. Republishing through the exchange would deliver to every queue bound to
 * it, so replaying one failure in one consumer would hand duplicate work to every other consumer
 * that had already succeeded.
 *
 * <h2>What it does not do</h2>
 *
 * <p>Replay is at-least-once, like everything else here. Each message is published to the source
 * queue and only then acknowledged in the dead-letter queue; if the process dies between the two,
 * that message is replayed again on the next run. The alternative — acknowledging first — loses
 * it, which is the wrong way round for a tool whose entire job is not losing things.
 *
 * <p>It does not fix anything. Replaying into a consumer that still has the bug simply refills
 * the dead-letter queue, more slowly than the first time and with the replay count climbing.
 */
public final class Replay {

    private static final Logger log = LoggerFactory.getLogger(Replay.class);

    /**
     * How long to wait for the next message before deciding the queue is empty.
     *
     * <p>Short, because an empty queue is the normal way a drain ends and every run pays this
     * once. It is not a correctness knob: a message arriving later is simply left for the next
     * run rather than lost.
     */
    private static final Duration EMPTY_QUEUE_TIMEOUT = Duration.ofMillis(200);

    private final TransportConnection connection;
    private final String sourceQueue;
    private final String fromQueue;

    Replay(TransportConnection connection, String sourceQueue, String fromQueue) {
        this.connection = connection;
        this.sourceQueue = sourceQueue;
        this.fromQueue = fromQueue;
    }

    /**
     * Switches to the parking lot: messages that could not be decoded at all.
     *
     * <p>Separate from the dead-letter queue because the two need different fixes. A dead-lettered
     * message failed in a handler and may well succeed on a retry; a parked one could not be
     * turned into an object, so it will fail identically every time until the code that reads it
     * changes. Replaying the parking lot before deploying that change is pure churn.
     *
     * @return a replay reading the parking lot instead
     */
    public Replay parked() {
        return new Replay(connection, sourceQueue, sourceQueue + ".parked");
    }

    /**
     * Counts what is waiting, without moving anything.
     *
     * <p>Worth calling first. Replaying forty thousand messages into a consumer that is already
     * behind is a decision, and this is the number that decision needs.
     *
     * @return how many messages are in the queue being replayed
     */
    public long pending() {
        return connection.messageCount(fromQueue);
    }

    /** @return the queue messages are being taken from */
    public String from() {
        return fromQueue;
    }

    /** @return the queue messages are being returned to */
    public String to() {
        return sourceQueue;
    }

    /**
     * Moves every waiting message back, stopping when the queue is empty.
     *
     * <p>"Empty" means empty at the moment it is asked, so a queue still being filled will not
     * keep this running forever: what arrives after the drain reaches the end is left for the
     * next run.
     *
     * @return how many messages were moved
     */
    public int replayAll() {
        return replay(Integer.MAX_VALUE);
    }

    /**
     * Moves up to a given number of messages back.
     *
     * @param max most messages to move; the rest are left where they are
     * @return how many were actually moved, which is fewer than {@code max} when the queue ran out
     */
    public int replay(int max) {
        return replay(max, delivery -> true);
    }

    /**
     * Moves messages matching a filter, leaving the rest in place.
     *
     * <p>For the common case of one bad deployment among many failures: replay what failed with a
     * particular error, or what came from a particular origin, and leave the genuinely broken
     * messages to be looked at by a person.
     *
     * <p>A message the filter rejects is <strong>requeued, and the drain stops there</strong>.
     * That is deliberate and is the honest behaviour available: AMQP has no way to look past a
     * message without taking it, so skipping one means holding it while reading the next, and a
     * drain that holds every rejected message would run the queue's worth of unsettled deliveries
     * into memory. Stopping means a filter that rejects the first message moves nothing, which is
     * visible, rather than quietly reordering the queue, which is not.
     *
     * @param max most messages to move
     * @param filter decides whether a message should be moved
     * @return how many were moved
     */
    public int replay(int max, Predicate<InboundDelivery> filter) {
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1, was " + max);
        }
        Objects.requireNonNull(filter, "filter");

        int moved = 0;
        while (moved < max) {
            PulledMessage pulled = connection.receive(fromQueue, EMPTY_QUEUE_TIMEOUT).orElse(null);
            if (pulled == null) {
                break;
            }
            if (!filter.test(pulled.delivery())) {
                pulled.acknowledger().reject(true);
                log.debug("stopping the replay of {}: a message did not match the filter", fromQueue);
                break;
            }
            moveOne(pulled);
            moved++;
        }

        if (moved > 0) {
            log.info("replayed {} message(s) from {} back to {}", moved, fromQueue, sourceQueue);
        }
        return moved;
    }

    private void moveOne(PulledMessage pulled) {
        InboundDelivery delivery = pulled.delivery();
        OutboundMessage message = OutboundMessage.body(delivery.body())
                // The default exchange, addressing the queue by name. Going back through the
                // original exchange would fan the message out to every queue bound to it,
                // handing duplicate work to consumers that never failed.
                .exchange("")
                .routingKey(sourceQueue)
                .headers(replayHeaders(delivery))
                .messageId(delivery.messageId())
                .contentType(delivery.contentType())
                .build();

        ConfirmResult result;
        try {
            result = connection.send(message);
        } catch (RuntimeException e) {
            // Back to the dead-letter queue it came from. It is safer there than anywhere this
            // method could put it, and the next run will find it again.
            pulled.acknowledger().reject(true);
            throw new AceMqException("could not replay a message from " + fromQueue + " to " + sourceQueue
                    + "; it was left where it was", e);
        }

        if (!result.isConfirmed() || !result.isRouted()) {
            pulled.acknowledger().reject(true);
            throw new AceMqException("the broker did not accept a message replayed from " + fromQueue + " to "
                    + sourceQueue + ": " + result.detail() + ". It was left where it was.");
        }

        // Acknowledged only now. The window between the confirm and this line is the reason
        // replay is at-least-once: a crash here replays this message again next time, which is
        // the failure worth having.
        pulled.acknowledger().accept();
    }

    private Map<String, Object> replayHeaders(InboundDelivery delivery) {
        Map<String, Object> headers = new LinkedHashMap<>(delivery.headers());

        // Back to the first attempt, so the message gets the whole retry ladder again. Left
        // alone, it arrives already exhausted and the first failure dead-letters it straight
        // back -- a replay that cannot survive one bad moment is not much of a replay. One
        // rather than zero because an envelope's first attempt is numbered one, and a zero here
        // fails validation the moment a consumer reads it.
        headers.put(AceHeaders.ATTEMPT, 1);

        headers.put(AceHeaders.REPLAYED_FROM, fromQueue);
        headers.put(AceHeaders.REPLAYED_AT, Instant.now().toEpochMilli());
        headers.put(AceHeaders.REPLAY_COUNT, previousReplays(delivery) + 1);

        // The error that put it here stays readable. Clearing it would hide the one piece of
        // context that explains why this message looks different from its neighbours.
        return headers;
    }

    private static int previousReplays(InboundDelivery delivery) {
        Object count = delivery.headers().get(AceHeaders.REPLAY_COUNT);
        if (count instanceof Number) {
            return ((Number) count).intValue();
        }
        if (count != null) {
            try {
                return Integer.parseInt(count.toString());
            } catch (NumberFormatException e) {
                // A header somebody else wrote. Counting from zero is better than failing the
                // replay over a number that is only used for reporting.
                return 0;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Replay{from=" + fromQueue + ", to=" + sourceQueue + "}";
    }
}
