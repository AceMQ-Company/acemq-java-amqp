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

import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionConfig;
import org.junit.jupiter.api.AfterEach;

/** {@link OnBehalfPublishContract} against the in-memory broker. */
class WithoutConfirmsTest extends OnBehalfPublishContract {

    @Override
    AceMq connect() {
        return AceMq.connect(
                ConnectionConfig.url("memory://without-confirms").withoutPublisherConfirms().build(), Telemetry.NONE);
    }

    @AfterEach
    void reset() {
        InMemoryTransport.reset();
    }
}
