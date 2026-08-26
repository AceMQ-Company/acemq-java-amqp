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

/** A running consumer. */
public interface MessageConsumer extends AutoCloseable {

    /** @return the queue being consumed */
    String queue();

    /** @return whether it is still receiving deliveries */
    boolean isRunning();

    /** @return how many messages have been acknowledged */
    long acknowledged();

    /** @return how many messages have been rejected after a handler failure */
    long rejected();

    /**
     * @return how many deliveries were recognised as already handled and acknowledged without
     *     running the handler; zero unless an idempotency store is configured
     */
    long duplicates();

    /** @return how many messages have been sent to a retry queue for another attempt */
    long retried();

    /** @return how many messages have been dead-lettered or parked */
    long deadLettered();

    /** Stops consuming, letting in-flight deliveries settle. */
    @Override
    void close();
}
