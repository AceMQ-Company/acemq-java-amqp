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
 * A scrape surface for an application that has no HTTP server of its own.
 *
 * <p>Three paths, the same three every AceMQ library serves, so a scrape configuration or a
 * probe written against one works against another:
 *
 * <pre>
 *   /acemq-metrics   Prometheus text format
 *   /acemq-health    JSON, 503 when the connection is down
 *   /acemq-info      JSON: library version, application version, transport capabilities
 * </pre>
 *
 * <p>Use it in a worker, a daemon or a command-line consumer. A Spring Boot application should
 * not: Actuator already gives it a {@code MeterRegistry} and an endpoint to serve from, and
 * {@code org.acemq.amqp.core.MicrometerSupport} already feeds that registry.
 *
 * @see org.acemq.amqp.actuator.AceMqActuator
 */

@org.jspecify.annotations.NullMarked
package org.acemq.amqp.actuator;
