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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.acemq.amqp.api.AceMqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sequence of steps where each one knows how to undo itself.
 *
 * <pre>{@code
 * Saga<Order> booking = Saga.named("place-order")
 *         .step("take-payment", order -> payments.charge(order))
 *                 .compensateWith(order -> payments.refund(order))
 *         .step("reserve-stock", order -> inventory.reserve(order))
 *                 .compensateWith(order -> inventory.release(order))
 *         .step("book-courier", order -> couriers.book(order))
 *         .build();
 *
 * SagaResult result = booking.run(order);
 * }</pre>
 *
 * <p>If {@code book-courier} throws, the stock is released and the payment refunded, in that
 * order, and {@link SagaResult#compensated()} says so.
 *
 * <h2>What this is not</h2>
 *
 * <p><strong>Not a distributed transaction.</strong> Nothing is isolated: after
 * {@code take-payment} the customer's money really has moved, and anybody looking sees that it
 * has. If {@code book-courier} then fails, the refund is a <em>new</em> fact rather than an
 * erasure of the old one, and for a few seconds the world contained a charge that should not
 * have happened. That is not a defect in this class; it is what compensating a real-world action
 * means, and a saga is honest about it where a two-phase commit pretends otherwise.
 *
 * <p>So the steps must be things that can be undone by doing something else. Sending an email
 * cannot be compensated — the apology is a second email, not an unsend — and a saga step that
 * sends one should be the last step, after everything that can still fail.
 *
 * <p><strong>Not durable.</strong> This runs in one process and its state is on the stack. A
 * crash midway leaves the saga half-applied with nothing to resume it, which is the honest
 * limitation of the in-process form and is why the compensations run in a {@code finally}-shaped
 * path rather than after a restart. Where a saga must survive the process, the steps have to be
 * messages and the state has to be in a database — a much larger thing, and it is not this.
 *
 * <p>For most systems the in-process form is the right one: it turns "remember to undo the three
 * things you already did" from a comment into something the compiler can see.
 *
 * <h2>When compensation itself fails</h2>
 *
 * <p>It is tried, it is logged, and the remaining compensations still run. The alternative —
 * stopping — leaves more undone than continuing does. What comes back is a
 * {@link SagaResult} listing what could not be undone, and that list is the thing to alert on:
 * it is the set of facts a human now has to reconcile by hand.
 *
 * @param <T> what the saga operates on
 */
public final class Saga<T> {

    private static final Logger log = LoggerFactory.getLogger(Saga.class);

    private final String name;
    private final List<Step<T>> steps;

    private Saga(String name, List<Step<T>> steps) {
        this.name = name;
        this.steps = steps;
    }

    /**
     * @param name what this saga is called, in logs and results
     * @param <T> what it operates on
     * @return a builder
     */
    public static <T> Builder<T> named(String name) {
        return new Builder<>(Objects.requireNonNull(name, "name"), new ArrayList<>());
    }

    /**
     * Runs the steps, compensating in reverse if one fails.
     *
     * @param subject what to operate on
     * @return what happened; never throws for a step failure, because a caller needs the
     *     compensation report more than it needs a stack trace
     */
    public SagaResult run(T subject) {
        List<String> completed = new ArrayList<>();

        for (Step<T> step : steps) {
            try {
                step.action.accept(subject);
                completed.add(step.name);
                log.debug("saga {} completed step {}", name, step.name);
            } catch (RuntimeException failure) {
                log.warn("saga {} failed at step {}: {}", name, step.name, failure.toString());
                List<String> unresolved = compensate(subject, completed);
                return SagaResult.compensated(name, step.name, failure, completed, unresolved);
            }
        }
        return SagaResult.completed(name, completed);
    }

    /**
     * Undoes what was done, most recent first.
     *
     * <p>Reverse order because that is the order the world was changed in, and a compensation
     * often depends on the state a later step has not yet altered.
     *
     * @return the steps whose compensation failed, which is what a human has to reconcile
     */
    private List<String> compensate(T subject, List<String> completed) {
        List<String> unresolved = new ArrayList<>();

        for (int i = completed.size() - 1; i >= 0; i--) {
            String name = completed.get(i);
            Step<T> step = stepNamed(name);
            if (step.compensation == null) {
                // Nothing to undo, which is legitimate: a step that only read something, or one
                // whose effect is harmless, needs no compensation.
                continue;
            }
            try {
                step.compensation.accept(subject);
                log.debug("saga {} compensated step {}", this.name, name);
            } catch (RuntimeException failure) {
                // Logged and carried on. Stopping here would leave more undone than continuing,
                // and the caller is told exactly which ones did not come back.
                log.error("saga {} could not compensate step {}: {}", this.name, name, failure.toString());
                unresolved.add(name);
            }
        }
        return unresolved;
    }

    private Step<T> stepNamed(String name) {
        for (Step<T> step : steps) {
            if (step.name.equals(name)) {
                return step;
            }
        }
        throw new AceMqException("saga " + this.name + " has no step called '" + name + "'");
    }

    /** @return the step names, in order */
    public List<String> stepNames() {
        List<String> names = new ArrayList<>();
        for (Step<T> step : steps) {
            names.add(step.name);
        }
        return names;
    }

    @Override
    public String toString() {
        return "Saga{" + name + ": " + String.join(" -> ", stepNames()) + "}";
    }

    /** One step and the thing that undoes it. */
    private static final class Step<T> {

        private final String name;
        private final Consumer<T> action;
        private final Consumer<T> compensation;

        Step(String name, Consumer<T> action, Consumer<T> compensation) {
            this.name = name;
            this.action = action;
            this.compensation = compensation;
        }
    }

    /** Collects the steps of a {@link Saga}. */
    public static final class Builder<T> {

        private final String name;
        private final List<Step<T>> steps;

        private Builder(String name, List<Step<T>> steps) {
            this.name = name;
            this.steps = steps;
        }

        /**
         * Adds a step with no compensation.
         *
         * <p>Legitimate for a step that changed nothing, and a mistake for one that did. There
         * is no warning for the second case, because a library cannot tell them apart — which
         * is the argument for writing the compensation first and the action second.
         *
         * @param stepName what the step is called
         * @param action the work
         * @return this builder
         */
        public Builder<T> step(String stepName, Consumer<T> action) {
            Objects.requireNonNull(stepName, "stepName");
            Objects.requireNonNull(action, "action");
            for (Step<T> existing : steps) {
                if (existing.name.equals(stepName)) {
                    throw new IllegalArgumentException("saga " + name + " already has a step called '"
                            + stepName + "'. Names identify a step in the compensation report, so two"
                            + " of them would make that report ambiguous.");
                }
            }
            steps.add(new Step<>(stepName, action, null));
            return this;
        }

        /**
         * Gives the most recently added step something that undoes it.
         *
         * @param compensation how to undo the last step
         * @return this builder
         */
        public Builder<T> compensateWith(Consumer<T> compensation) {
            Objects.requireNonNull(compensation, "compensation");
            if (steps.isEmpty()) {
                throw new IllegalStateException("there is no step to compensate yet: call step(...) first");
            }
            Step<T> last = steps.remove(steps.size() - 1);
            steps.add(new Step<>(last.name, last.action, compensation));
            return this;
        }

        /**
         * @return the saga
         * @throws IllegalStateException if it has no steps
         */
        public Saga<T> build() {
            if (steps.isEmpty()) {
                throw new IllegalStateException("saga " + name + " has no steps");
            }
            return new Saga<>(name, Collections.unmodifiableList(new ArrayList<>(steps)));
        }
    }
}
