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

import org.jspecify.annotations.Nullable;

/**
 * One stage of a pipeline: a message in, the next payload out.
 *
 * <pre>{@code
 * class EnrichOrder implements Step<Order, Enriched> {
 *     public Enriched handle(Message<Order> message) {
 *         Order order = message.payload();
 *         return new Enriched(order, customers.lookup(order.customerId()));
 *     }
 * }
 * }</pre>
 *
 * <p>Returning the next payload rather than publishing it keeps a step testable — it is a
 * function, and a test calls it — and lets the builder thread types along the chain, so a step
 * that produces an {@code Enriched} cannot be put where one producing an {@code Order} belongs.
 *
 * <p><strong>Returning {@code null} ends the route for that message.</strong> A step that filters
 * is a step that sometimes returns nothing; the message is acknowledged and nothing downstream
 * ever sees it. That is a decision, not a failure, and it is counted separately from both.
 *
 * <p>A step should be idempotent. Every hop is at-least-once, so a step sees the same message
 * twice whenever a publish succeeds and its acknowledgement does not — configure an idempotency
 * store on the ones where repeating the work would matter.
 *
 * @param <I> payload this step receives
 * @param <O> payload it produces for the next step
 */
@FunctionalInterface
public interface Step<I, O> {

    /**
     * @param message the message to handle
     * @return the payload for the next step, or {@code null} to end the route here
     * @throws Exception to fail this step, applying whatever failure policy it was configured
     *     with
     */
    @Nullable
    O handle(Message<I> message) throws Exception;
}
