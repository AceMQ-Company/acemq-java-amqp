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

import java.time.Duration;

/** The broker's answer to one publish. */
public final class ConfirmResult {

    private final boolean confirmed;
    private final boolean routed;
    private final Duration latency;
    private final String detail;

    private ConfirmResult(boolean confirmed, boolean routed, Duration latency, String detail) {
        this.confirmed = confirmed;
        this.routed = routed;
        this.latency = latency;
        this.detail = detail;
    }

    /** The broker took responsibility for the message and something was bound to receive it. */
    public static ConfirmResult confirmed(Duration latency) {
        return new ConfirmResult(true, true, latency, null);
    }

    /**
     * The broker took responsibility for the message but nothing was bound to receive it.
     *
     * <p>Reported separately from a failure because the publish did succeed; whether an
     * unroutable message is an error is the core's decision, not the transport's.
     */
    public static ConfirmResult unroutable(Duration latency, String detail) {
        return new ConfirmResult(true, false, latency, detail);
    }

    /** The broker refused the message or never answered. */
    public static ConfirmResult failed(Duration latency, String detail) {
        return new ConfirmResult(false, false, latency, detail);
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public boolean isRouted() {
        return routed;
    }

    public Duration latency() {
        return latency;
    }

    /** @return why the publish was unroutable or failed, when the broker said */
    public String detail() {
        return detail;
    }

    @Override
    public String toString() {
        return "ConfirmResult{confirmed=" + confirmed + ", routed=" + routed + ", latency="
                + latency + (detail == null ? "" : ", detail=" + detail) + "}";
    }
}
