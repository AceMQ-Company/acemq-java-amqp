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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What applying a {@link Topology} would do, worked out before anything is done.
 *
 * <p>The point of producing this rather than simply declaring is that it can be read.
 * Declaring blindly is how a topology change reaches production unreviewed and fails
 * there; a plan can be printed in a build log, attached to a pull request, or refused.
 */
public final class TopologyPlan {

    private final List<Action> actions;

    /**
     * @param actions what the plan proposes, in the order it would be carried out
     * @return the plan
     */
    public static TopologyPlan of(List<Action> actions) {
        return new TopologyPlan(actions);
    }

    private TopologyPlan(List<Action> actions) {
        this.actions = Collections.unmodifiableList(new ArrayList<>(actions));
    }

    public List<Action> actions() {
        return actions;
    }

    /** @return only the actions that would change the broker */
    public List<Action> changes() {
        List<Action> changes = new ArrayList<>();
        for (Action action : actions) {
            if (action.kind() == Kind.CREATE) {
                changes.add(action);
            }
        }
        return changes;
    }

    /** @return whether applying this plan would change anything */
    public boolean hasChanges() {
        return !changes().isEmpty();
    }

    /**
     * Renders the plan the way a build log or a review wants to read it.
     *
     * @return a human-readable, line-per-action rendering
     */
    public String render() {
        StringBuilder text = new StringBuilder("topology plan:\n");
        if (actions.isEmpty()) {
            return text.append("  nothing declared\n").toString();
        }
        for (Action action : actions) {
            text.append("  ")
                    .append(action.kind() == Kind.CREATE ? "create " : "present")
                    .append("  ")
                    .append(action.description())
                    .append('\n');
        }
        return text.toString();
    }

    @Override
    public String toString() {
        return "TopologyPlan{" + changes().size() + " change(s) of " + actions.size() + " item(s)}";
    }

    /** What a plan proposes to do about one item. */
    public enum Kind {
        /** The item is missing and would be created. */
        CREATE,
        /** The item already exists and would be left alone. */
        PRESENT
    }

    /** One item of a plan. */
    public static final class Action {

        private final Kind kind;
        private final String description;

        /**
         * @param kind whether the item would be created or is already present
         * @param description what the item is, as a plan reader wants to see it
         * @return the action
         */
        public static Action of(Kind kind, String description) {
            return new Action(kind, description);
        }

        private Action(Kind kind, String description) {
            this.kind = kind;
            this.description = description;
        }

        public Kind kind() {
            return kind;
        }

        public String description() {
            return description;
        }

        @Override
        public String toString() {
            return kind + " " + description;
        }
    }
}
