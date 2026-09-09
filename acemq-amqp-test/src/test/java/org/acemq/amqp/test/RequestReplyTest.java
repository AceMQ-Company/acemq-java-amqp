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
package org.acemq.amqp.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.RequestTimedOutException;
import org.acemq.amqp.core.Requester;
import org.acemq.amqp.core.Responder;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.PulledMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("request and reply")
class RequestReplyTest {

    // Plain classes rather than records: this library targets Java 11 bytecode so Spring Boot 2.7
    // applications can consume it, and records need 16. See ADR-015. The examples are Java 17 and
    // use records freely, which is why the same shape looks different there.
    static final class Quote {

        private String sku;
        private int quantity;

        Quote() {
            // for the codec
        }

        Quote(String sku, int quantity) {
            this.sku = sku;
            this.quantity = quantity;
        }

        public String getSku() {
            return sku;
        }

        public void setSku(String sku) {
            this.sku = sku;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }

    static final class Price {

        private String sku;
        private double amount;

        Price() {
            // for the codec
        }

        Price(String sku, double amount) {
            this.sku = sku;
            this.amount = amount;
        }

        public String getSku() {
            return sku;
        }

        public void setSku(String sku) {
            this.sku = sku;
        }

        public double getAmount() {
            return amount;
        }

        public void setAmount(double amount) {
            this.amount = amount;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Price)) {
                return false;
            }
            Price that = (Price) other;
            return Double.compare(amount, that.amount) == 0 && java.util.Objects.equals(sku, that.sku);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sku, amount);
        }

        @Override
        public String toString() {
            return "Price{" + sku + ", " + amount + "}";
        }
    }

    private AceMq mq;

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        mq.declareQueue("pricing", QueueType.CLASSIC, Collections.emptyMap());
        return mq;
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("the round trip")
    class RoundTrip {

        @Test
        @Timeout(30)
        void an_answer_comes_back_to_the_caller_that_asked() {
            connect("rr-basic");
            try (Responder responder = mq.respond("pricing", Quote.class,
                    quote -> new Price(quote.getSku(), quote.getQuantity() * 2.5));
                    Requester requester = mq.requester()) {

                Price price = requester.request("", "pricing", new Quote("WIDGET", 4), Price.class,
                        Duration.ofSeconds(10));

                assertThat(price).isEqualTo(new Price("WIDGET", 10.0));
                // The responder counts after publishing, so the caller can return first.
                await().atMost(Duration.ofSeconds(10)).until(() -> responder.answered() == 1);
            }
        }

        @Test
        @Timeout(30)
        void several_questions_in_flight_get_their_own_answers() {
            connect("rr-concurrent");
            try (Responder responder = mq.respond("pricing", Quote.class,
                    ConsumerOptions.prefetch(10),
                    quote -> new Price(quote.getSku(), quote.getQuantity()));
                    Requester requester = mq.requester()) {

                // The claim correlation ids exist for: three answers, matched to the right
                // three callers. A requester that returned whatever arrived first would pass
                // a single-request test and corrupt every real system.
                List<CompletableFuture<Price>> pending = new CopyOnWriteArrayList<>();
                for (int i = 1; i <= 3; i++) {
                    pending.add(requester.requestAsync("", "pricing", new Quote("SKU-" + i, i), Price.class));
                }

                assertThat(pending.get(0).join()).isEqualTo(new Price("SKU-1", 1.0));
                assertThat(pending.get(1).join()).isEqualTo(new Price("SKU-2", 2.0));
                assertThat(pending.get(2).join()).isEqualTo(new Price("SKU-3", 3.0));
            }
        }

        @Test
        @Timeout(30)
        void the_reply_queue_is_cleaned_up_when_the_requester_closes() {
            connect("rr-cleanup");
            String replyQueue;
            try (Requester requester = mq.requester()) {
                replyQueue = requester.replyQueue();
                assertThat(mq.messageCount(replyQueue)).isZero();
            }
            // Gone, not merely empty. A reply queue outliving its owner holds answers
            // nobody will ever read.
            assertThatThrownBy(() -> mq.messageCount(replyQueue)).isInstanceOf(RuntimeException.class);
        }
    }

    @Nested
    @DisplayName("when no answer comes")
    class NoAnswer {

        @Test
        @Timeout(30)
        void the_caller_is_told_how_long_it_waited_and_what_that_does_not_mean() {
            connect("rr-timeout");
            try (Requester requester = mq.requester()) {
                // Nothing is serving the queue.
                assertThatThrownBy(() -> requester.request("", "pricing", new Quote("WIDGET", 1),
                        Price.class, Duration.ofMillis(300)))
                        .isInstanceOf(RequestTimedOutException.class)
                        // The message matters as much as the type: a timeout does not mean the
                        // request was not handled, and retrying is a decision about idempotency.
                        .hasMessageContaining("may still be queued")
                        .hasMessageContaining("idempotent");

                assertThat(requester.timedOut()).isEqualTo(1);
            }
        }

        @Test
        @Timeout(30)
        void a_late_reply_is_counted_rather_than_handed_to_nobody() {
            connect("rr-late");
            try (Requester requester = mq.requester()) {
                assertThatThrownBy(() -> requester.request("", "pricing", new Quote("W", 1),
                        Price.class, Duration.ofMillis(200)))
                        .isInstanceOf(RequestTimedOutException.class);

                // The responder starts only now, so its answer arrives after the caller gave up.
                try (Responder responder = mq.respond("pricing", Quote.class,
                        quote -> new Price(quote.getSku(), 1))) {
                    await().atMost(Duration.ofSeconds(10)).until(() -> requester.unmatched() == 1);
                }
                // Worth graphing: a rising count here says the timeout is too short, not that
                // anything is broken.
                assertThat(requester.unmatched()).isEqualTo(1);
            }
        }

        @Test
        @Timeout(30)
        void closing_the_requester_releases_anyone_still_waiting() {
            connect("rr-close");
            CompletableFuture<Price> pending;
            try (Requester requester = mq.requester()) {
                pending = requester.requestAsync("", "pricing", new Quote("W", 1), Price.class);
            }
            // Failed rather than left hanging: a future nobody will ever complete is a thread
            // parked until its own timeout, if it had one.
            assertThat(pending).isCompletedExceptionally();
        }
    }

    @Nested
    @DisplayName("where the answer is addressed")
    class ReplyAddress {

        /**
         * Publishes a request the way some other library would, naming the reply queue in only
         * one of the two places, and returns the correlation id it used.
         *
         * <p>Raw rather than through a publisher, because a publisher writes both and the whole
         * question here is what happens when only one of them arrives.
         */
        private String ask(TransportConnection raw, String replyQueue, boolean header, boolean property) {
            String id = java.util.UUID.randomUUID().toString();
            OutboundMessage.Builder request = OutboundMessage
                    .body("{\"sku\":\"WIDGET\",\"quantity\":4}".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .exchange("")
                    .routingKey("pricing")
                    .messageId(id)
                    .contentType("application/json")
                    .header(AceHeaders.ID, id)
                    .header(AceHeaders.TYPE, "Quote")
                    .header(AceHeaders.CORRELATION, id);
            if (header) {
                request.header(AceHeaders.REPLY_TO, replyQueue);
            }
            if (property) {
                request.replyTo(replyQueue);
            }
            assertThat(raw.send(request.build()).isConfirmed()).isTrue();
            return id;
        }

        private void expectAnAnswerFor(String broker, boolean header, boolean property) {
            connect(broker);
            mq.declareQueue("replies", QueueType.CLASSIC, Collections.emptyMap());

            try (TransportConnection raw = new InMemoryTransport()
                    .connect(ConnectionConfig.url("memory://" + broker).build());
                    Responder responder = mq.respond("pricing", Quote.class,
                            quote -> new Price(quote.getSku(), quote.getQuantity() * 2.5))) {

                String id = ask(raw, "replies", header, property);

                await().atMost(Duration.ofSeconds(10)).until(() -> responder.answered() == 1);
                assertThat(responder.unanswerable()).isZero();

                PulledMessage reply = raw.receive("replies", Duration.ofSeconds(10))
                        .orElseThrow(() -> new AssertionError("no answer arrived on the reply queue"));
                assertThat(reply.delivery().headers()).containsEntry(AceHeaders.CORRELATION, id);
                reply.acknowledger().accept();
            }
        }

        @Test
        @Timeout(30)
        void a_request_carrying_only_the_native_property_is_answered() {
            // What Java and .NET have always published, and what a service that never heard of
            // AceMQ publishes. A responder that read only the header would leave it unanswered.
            expectAnAnswerFor("rr-native-only", false, true);
        }

        @Test
        @Timeout(30)
        void a_request_carrying_only_the_header_is_answered() {
            // What Go, Python and Ruby have always published. Until the header was read, a Java
            // responder counted these as unanswerable and the caller waited out its timeout --
            // the bug that made a Java requester and a Go responder unable to talk at all.
            expectAnAnswerFor("rr-header-only", true, false);
        }

        @Test
        @Timeout(30)
        void a_request_this_library_publishes_names_the_queue_in_both_places() {
            // The other half of "write both, read either". A new library has to be answerable
            // by an old one as well as the other way round, and that only works if the pair is
            // always written together and always agrees.
            connect("rr-writes-both");
            try (TransportConnection raw = new InMemoryTransport()
                    .connect(ConnectionConfig.url("memory://rr-writes-both").build())) {
                String replyQueue;
                try (Requester requester = mq.requester()) {
                    replyQueue = requester.replyQueue();
                    requester.requestAsync("", "pricing", new Quote("WIDGET", 4), Price.class);

                    PulledMessage request = raw.receive("pricing", Duration.ofSeconds(10))
                            .orElseThrow(() -> new AssertionError("the request never arrived"));
                    assertThat(request.delivery().replyTo()).hasValue(replyQueue);
                    assertThat(request.delivery().headers()).containsEntry(AceHeaders.REPLY_TO, replyQueue);
                    request.acknowledger().accept();
                }
            }
        }
    }

    @Nested
    @DisplayName("a request nobody can answer")
    class NoReplyTo {

        @Test
        @Timeout(30)
        void is_counted_and_acknowledged_rather_than_retried_forever() {
            connect("rr-noreply");
            try (Responder responder = mq.respond("pricing", Quote.class,
                    quote -> new Price(quote.getSku(), 1))) {

                // Published, not requested: no reply-to. Failing here would retry it up the
                // ladder and dead-letter it, when the sender is the thing that is wrong.
                mq.publisher("", "pricing", Quote.class).send(new Quote("WIDGET", 1));

                await().atMost(Duration.ofSeconds(10)).until(() -> responder.unanswerable() == 1);
                assertThat(responder.answered()).isZero();
            }
        }
    }
}
