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

/**
 * A feature a broker either supports or does not.
 *
 * <p>AceMQ never silently emulates a missing capability. The core asks the transport
 * what the connected broker can do and then either uses the native feature, applies a
 * documented alternative, or fails at startup naming the capability. This is what makes
 * one API safe to point at both RabbitMQ and an AMQP 1.0 broker.
 */
public enum Capability {

    /** Publish to an exchange that routes to queues, rather than directly to a node. */
    EXCHANGE_ROUTING,

    /** Topic-style routing keys with wildcard bindings. */
    TOPIC_WILDCARDS,

    /** Routing decided by message headers rather than by routing key. */
    HEADERS_ROUTING,

    /** Broker acknowledges that a published message was taken responsibility for. */
    PUBLISHER_CONFIRMS,

    /** Multi-message transactions on the publishing side. */
    TRANSACTIONS,

    /** Native dead-letter routing configured on the queue. */
    DEAD_LETTER_NATIVE,

    /** Per-message time to live, as opposed to a queue-wide setting. */
    TTL_PER_MESSAGE,

    /** Priority ordering within a queue. */
    PRIORITY,

    /** Replicated queues with a consensus protocol. */
    QUORUM_QUEUES,

    /** Append-only, replayable log semantics with consumer-held offsets. */
    STREAMS,

    /** Delivery deferred until a requested time, without occupying a consumer. */
    DELAYED_DELIVERY,

    /** At most one active consumer at a time, preserving order across failover. */
    SINGLE_ACTIVE_CONSUMER,

    /** Routing that distributes keys deterministically across queues. */
    CONSISTENT_HASH_ROUTING
}
