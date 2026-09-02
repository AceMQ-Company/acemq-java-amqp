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

/**
 * Payload encryption, for messages the broker itself should not be able to read.
 *
 * <p>TLS protects a message while it is moving and does nothing about one sitting in a queue
 * that an operator, a backup or the management UI can read. Where that matters, the message has
 * to arrive already encrypted, and a {@link org.acemq.amqp.api.Codec} is the seam for it: the
 * last thing to touch the bytes going out and the first coming in.
 */

@org.jspecify.annotations.NullMarked
package org.acemq.amqp.crypto;
