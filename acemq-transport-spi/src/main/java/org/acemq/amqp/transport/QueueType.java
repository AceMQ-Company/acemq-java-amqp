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
package org.acemq.amqp.transport;

/** The kind of queue to declare. */
public enum QueueType {

    /** A single-node queue. Fast, and lost with its node. */
    CLASSIC,

    /** A replicated queue with a consensus protocol. The default for anything durable. */
    QUORUM,

    /** An append-only, replayable log with consumer-held offsets. */
    STREAM
}
