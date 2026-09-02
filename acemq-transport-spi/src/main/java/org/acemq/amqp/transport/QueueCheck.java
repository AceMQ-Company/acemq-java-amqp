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

import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * What a broker says about a queue somebody is about to declare.
 *
 * <p>Four answers rather than two, because "the queue is not the way you want it" and "I cannot
 * tell you" are different facts and collapsing them would make the second look like the first.
 * A transport that cannot inspect a queue says {@link #unsupported()}, and callers report that
 * as unknown rather than as agreement.
 */
public final class QueueCheck {

    /** Which of the four answers this is. */
    public enum Result {

        /** The queue is there and would accept the declaration unchanged. */
        MATCHES,

        /** No queue by that name. */
        ABSENT,

        /** There, with settings that cannot be changed in place. */
        DIFFERS,

        /** This transport cannot tell. Not the same as agreement. */
        UNSUPPORTED
    }

    private static final QueueCheck MATCHES = new QueueCheck(Result.MATCHES, null);
    private static final QueueCheck ABSENT = new QueueCheck(Result.ABSENT, null);
    private static final QueueCheck UNSUPPORTED = new QueueCheck(Result.UNSUPPORTED, null);

    private final Result result;
    private final @Nullable String detail;

    private QueueCheck(Result result, @Nullable String detail) {
        this.result = result;
        this.detail = detail;
    }

    /** @return the queue exists and matches */
    public static QueueCheck matches() {
        return MATCHES;
    }

    /** @return there is no such queue */
    public static QueueCheck absent() {
        return ABSENT;
    }

    /**
     * @param detail what differs, in the broker's own words where there are any. This ends up
     *     in front of whoever is deciding what to do about it, so it should name the argument
     *     rather than say that something is wrong
     * @return the queue exists with settings that would be refused
     */
    public static QueueCheck differs(String detail) {
        return new QueueCheck(Result.DIFFERS, Objects.requireNonNull(detail, "detail"));
    }

    /** @return this transport cannot answer the question */
    public static QueueCheck unsupported() {
        return UNSUPPORTED;
    }

    /** @return which answer this is */
    public Result result() {
        return result;
    }

    /** @return what differs, when something does */
    public @Nullable String detail() {
        return detail;
    }

    @Override
    public String toString() {
        return detail == null ? result.name() : result + "(" + detail + ")";
    }
}
