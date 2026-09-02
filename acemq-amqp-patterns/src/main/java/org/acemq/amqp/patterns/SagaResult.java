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
package org.acemq.amqp.patterns;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * What a saga did.
 *
 * <p>Returned rather than thrown, because a failed saga is not an exceptional condition to a
 * caller that has to decide what happens next — and because the interesting part is not the
 * exception but {@link #unresolved()}, the list of things that could not be undone.
 */
public final class SagaResult {

    private final String saga;
    private final @Nullable String failedAt;
    private final @Nullable RuntimeException failure;
    private final List<String> completed;
    private final List<String> unresolved;

    private SagaResult(
            String saga,
            @Nullable String failedAt,
            @Nullable RuntimeException failure,
            List<String> completed,
            List<String> unresolved) {
        this.saga = saga;
        this.failedAt = failedAt;
        this.failure = failure;
        this.completed = Collections.unmodifiableList(completed);
        this.unresolved = Collections.unmodifiableList(unresolved);
    }

    static SagaResult completed(String saga, List<String> completed) {
        return new SagaResult(saga, null, null, completed, Collections.emptyList());
    }

    static SagaResult compensated(
            String saga,
            String failedAt,
            RuntimeException failure,
            List<String> completed,
            List<String> unresolved) {
        return new SagaResult(saga, failedAt, failure, completed, unresolved);
    }

    /** @return whether every step ran */
    public boolean isComplete() {
        return failedAt == null;
    }

    /** @return whether a step failed and the earlier ones were undone */
    public boolean compensated() {
        return failedAt != null;
    }

    /** @return the step that failed, when one did */
    public Optional<String> failedAt() {
        return Optional.ofNullable(failedAt);
    }

    /** @return why it failed */
    public Optional<RuntimeException> failure() {
        return Optional.ofNullable(failure);
    }

    /** @return the steps that ran, in order, before the failure */
    public List<String> completed() {
        return completed;
    }

    /**
     * The steps whose compensation failed.
     *
     * <p><strong>This is the list to alert on.</strong> Everything else a saga reports is
     * recoverable by construction; these are real-world effects that happened, were meant to be
     * undone, and were not. Nothing else in the system knows about them, and no retry will
     * resolve them — a person has to.
     *
     * @return the steps a human now has to reconcile, in the order compensation was attempted
     */
    public List<String> unresolved() {
        return unresolved;
    }

    /** @return whether anything was left in a state nobody intended */
    public boolean hasUnresolved() {
        return !unresolved.isEmpty();
    }

    @Override
    public String toString() {
        if (isComplete()) {
            return "SagaResult{" + saga + " completed: " + String.join(" -> ", completed) + "}";
        }
        return "SagaResult{" + saga + " failed at " + failedAt
                + ", compensated " + completed
                + (unresolved.isEmpty() ? "" : ", UNRESOLVED " + unresolved) + "}";
    }
}
