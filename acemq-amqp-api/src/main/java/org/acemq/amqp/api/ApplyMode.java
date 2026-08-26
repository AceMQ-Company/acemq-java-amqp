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

/** How much a topology apply is allowed to do. */
public enum ApplyMode {

    /**
     * Work out the plan and change nothing.
     *
     * <p>What a build should run in review, and what a cautious start-up should log.
     */
    DRY_RUN,

    /**
     * Create what is missing and leave everything else alone.
     *
     * <p>The default, and deliberately not destructive: AMQP forbids changing most queue
     * arguments in place, so a change that looks like an edit is really a delete and a
     * recreate, which discards messages. That is a migration, and a migration should be a
     * decision rather than a side effect of a start-up.
     */
    CREATE_ONLY,

    /**
     * Change nothing and fail if anything is missing.
     *
     * <p>For an environment where topology is provisioned separately and an application
     * finding it absent is a deployment error rather than something to fix silently.
     */
    VALIDATE
}
