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

import org.testcontainers.utility.DockerImageName;

/**
 * The RabbitMQ image the integration tests run against.
 *
 * <p>Defaults to the current major release and is overridden with
 * {@code -Dacemq.test.rabbitmq.image=rabbitmq:3.13-management}, which is how the compatibility
 * job in CI proves the library works on the previous one. Hardcoding the image in every test
 * meant "we support 3.13" was a claim nothing checked.
 *
 * <p>Deliberately duplicated in the two test source trees rather than shared: the test kit
 * already depends on this transport, so a module holding it that both could reach would close a
 * dependency cycle. Ten lines is the cheaper price.
 */
final class BrokerImage {

    private static final String PROPERTY = "acemq.test.rabbitmq.image";
    private static final String DEFAULT = "rabbitmq:4-management";

    private BrokerImage() {
    }

    /** @return the image to run, honouring the override */
    static DockerImageName current() {
        String configured = System.getProperty(PROPERTY, System.getenv("ACEMQ_RABBITMQ_IMAGE"));
        String image = configured == null || configured.isBlank() ? DEFAULT : configured;
        // Testcontainers matches its RabbitMQ wait strategy on the image name, so an image that
        // is not literally "rabbitmq" has to be told what it is standing in for.
        return DockerImageName.parse(image).asCompatibleSubstituteFor("rabbitmq");
    }
}
