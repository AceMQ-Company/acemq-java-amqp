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
 * The message header names that make up the AceMQ cross-language contract.
 *
 * <p>These names are the interoperability surface of the whole project. A Go publisher and
 * an Elixir consumer agree because they agree on these strings, and a service that does not
 * use AceMQ at all can still read them. They are frozen at specification version 1:
 * renaming one is a breaking change for every language port simultaneously.
 *
 * <p>Trace context deliberately does not carry the {@code x-acemq-} prefix, because
 * {@code traceparent} and {@code tracestate} are W3C names that other tools already know how
 * to read.
 */
public final class AceHeaders {

    /** Prefix shared by every AceMQ-defined header. */
    public static final String PREFIX = "x-acemq-";

    /** Unique message identifier, and the default idempotency key. */
    public static final String ID = PREFIX + "id";

    /** Logical message type, for example {@code order.placed}. */
    public static final String TYPE = PREFIX + "type";

    /** Schema version of the payload, as an integer. */
    public static final String VERSION = PREFIX + "version";

    /** Business correlation identifier, propagated unchanged across hops. */
    public static final String CORRELATION = PREFIX + "correlation";

    /** Identifier of the message that caused this one to be published. */
    public static final String CAUSATION = PREFIX + "causation";

    /** Delivery attempt counter, starting at 1 and incremented by the retry engine. */
    public static final String ATTEMPT = PREFIX + "attempt";

    /** Epoch milliseconds of the first publish, used for age-based give-up. */
    public static final String FIRST_SEEN = PREFIX + "first-seen";

    /** Identifier of the publishing process, conventionally {@code service@host}. */
    public static final String ORIGIN = PREFIX + "origin";

    /** URI of the externalised payload when the claim-check pattern is in use. */
    public static final String CLAIM = PREFIX + "claim";

    /** Why a message was dead-lettered. Present only on messages in a dead-letter queue. */
    public static final String ERROR = PREFIX + "error";

    /** Queue a message was replayed from, set by the replay API for auditing. */
    public static final String REPLAYED_FROM = PREFIX + "replayed-from";

    /** W3C trace context. Not prefixed, so non-AceMQ tooling recognises it. */
    public static final String TRACEPARENT = "traceparent";

    /** W3C trace state. Not prefixed, for the same reason as {@link #TRACEPARENT}. */
    public static final String TRACESTATE = "tracestate";

    private AceHeaders() {
        throw new AssertionError("AceHeaders is a constant holder and must not be instantiated");
    }

    /**
     * Reports whether a header name belongs to AceMQ.
     *
     * <p>Useful when copying headers between messages: AceMQ-owned headers are re-derived by
     * the engine rather than propagated verbatim.
     *
     * @param headerName header name to test, may be {@code null}
     * @return {@code true} if the name carries the {@link #PREFIX}
     */
    public static boolean isAceHeader(String headerName) {
        return headerName != null && headerName.startsWith(PREFIX);
    }
}
