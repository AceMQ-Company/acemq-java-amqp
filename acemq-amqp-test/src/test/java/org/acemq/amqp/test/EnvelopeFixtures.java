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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportConnection;

/**
 * Writes the envelope wire contract to JSON by publishing through the real library and then
 * pulling the message back at the transport level, which is the only place the engine's own
 * {@code x-acemq-*} headers are visible: the consumer API strips them and materialises them onto
 * the {@link Envelope}, by design.
 *
 * <p>Generated rather than transcribed. A port that hand-copies these from prose acquires a
 * difference nobody notices until two languages disagree in production.
 *
 * <p>This lived in {@code acemq-dotnet-amqp/tools/fixture-generator} and was run by hand with
 * javac while the .NET port was its only consumer. It belongs here, in the suite of the library
 * that defines the contract, so that {@link FixtureDriftTest} regenerates it on every build and
 * fails on the commit that changed the contract rather than months later in somebody else's
 * production.
 *
 * <p><strong>Three of the values it emits cannot be reproduced.</strong> The minimal case takes
 * its identifier from a fresh UUID, its {@code x-acemq-first-seen} from the clock and its
 * {@code x-acemq-origin} from the machine's hostname, because those are exactly the defaults the
 * contract says a caller who supplies nothing gets. The four ports read this file as input to
 * their own round trip rather than as literal expectations, so the particular values do not
 * matter to them — but it does mean the file is not byte-reproducible, and the drift test
 * compares it with those three values masked instead of pretending otherwise.
 */
final class EnvelopeFixtures {

    private EnvelopeFixtures() {
        throw new AssertionError("EnvelopeFixtures is a generator and must not be instantiated");
    }

    /**
     * Publishes the contract cases and renders them.
     *
     * @return the contents of {@code envelope-fixtures.json}
     */
    static String generate() {
        StringBuilder out = new StringBuilder();
        out.append("{\n  \"generatedBy\": \"acemq-java-amqp FixtureGen\",\n");
        out.append("  \"contract\": \"the headers an AceMQ publish puts on the wire\",\n");
        out.append("  \"cases\": [\n");

        // A broker name of its own, so a fixture run cannot pick up a message some other test
        // left behind and call it the contract.
        String broker = "memory://envelope-fixtures";
        try (AceMq mq = AceMq.connect(broker, Telemetry.NONE);
                TransportConnection raw = new InMemoryTransport().connect(ConnectionConfig.url(broker).build())) {

            mq.declareExchange("fx", "topic");
            mq.declareQueue("fx.q", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("fx.q", "fx", "fx.*");

            mq.publisher("fx", "fx.plain", String.class).send("{\"id\":\"o-1\"}");
            emit(out, "minimal", raw, true);

            Envelope rich = Envelope.of("order.placed")
                    .id("11111111-2222-3333-4444-555555555555")
                    .version(3)
                    .correlationId("corr-1")
                    .causationId("cause-1")
                    .origin("orders@host-7")
                    .firstSeen(Instant.parse("2026-01-02T03:04:05.678Z"))
                    .header("x-tenant", "acme")
                    .build();
            mq.publisher("fx", "fx.rich", String.class).send("{\"id\":\"o-2\"}", rich);
            emit(out, "populated", raw, false);
        }

        out.append("\n  ]\n}\n");
        return out.toString();
    }

    private static void emit(StringBuilder out, String name, TransportConnection raw, boolean first) {
        InboundDelivery m = raw.receive("fx.q", Duration.ofSeconds(5))
                .orElseThrow(() -> new IllegalStateException("nothing on the queue for " + name))
                .delivery();
        if (!first) {
            out.append(",\n");
        }
        out.append("    {\n      \"case\": ").append(q(name)).append(",\n");
        out.append("      \"routingKey\": ").append(q(m.routingKey())).append(",\n");
        out.append("      \"messageId\": ").append(q(m.messageId())).append(",\n");
        out.append("      \"body\": ").append(q(new String(m.body(), StandardCharsets.UTF_8))).append(",\n");
        out.append("      \"contentType\": ").append(q(m.contentType())).append(",\n");
        out.append("      \"headers\": {\n");
        List<String> keys = new ArrayList<>(m.headers().keySet());
        Collections.sort(keys);
        for (int i = 0; i < keys.size(); i++) {
            Object v = m.headers().get(keys.get(i));
            String rendered = (v instanceof Number || v instanceof Boolean) ? String.valueOf(v) : q(String.valueOf(v));
            // The two spaces before the separator are not a typo and are not cosmetic. Four
            // repositories hold a byte-identical copy of this file; changing the whitespace
            // changes every one of them for no gain at all.
            out.append("        ").append(q(keys.get(i))).append(": ").append(rendered)
                    .append("  ")
                    .append(i < keys.size() - 1 ? ",\n" : "\n");
        }
        out.append("      }\n    }");
    }

    private static String q(String s) {
        return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
