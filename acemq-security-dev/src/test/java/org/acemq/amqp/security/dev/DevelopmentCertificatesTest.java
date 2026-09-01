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
package org.acemq.amqp.security.dev;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("development certificates")
class DevelopmentCertificatesTest {

    private static final char[] PASSWORD = "acemq-dev".toCharArray();

    @Nested
    @DisplayName("what it writes")
    class Output {

        @Test
        @DisplayName("writes everything both sides need")
        void writesEverything(@TempDir Path directory) throws Exception {
            new DevelopmentCertificates().generate(directory, "localhost", PASSWORD, Duration.ofDays(30));

            assertThat(List.of("ca.crt", "ca.key", "server.crt", "server.key",
                    "client.crt", "client.key", "keystore.p12", "truststore.p12"))
                    .allSatisfy(name -> assertThat(directory.resolve(name)).exists());
        }

        @Test
        @DisplayName("the keystores open with the password and hold what they should")
        void keystoresAreUsable(@TempDir Path directory) throws Exception {
            new DevelopmentCertificates().generate(directory, "localhost", PASSWORD, Duration.ofDays(30));

            KeyStore keystore = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(directory.resolve("keystore.p12"))) {
                keystore.load(in, PASSWORD);
            }
            // A key entry rather than a bare certificate: this is the client's own identity,
            // and a store without the private key would fail only once mutual TLS was turned on.
            assertThat(keystore.isKeyEntry("acemq-client")).isTrue();

            KeyStore truststore = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(directory.resolve("truststore.p12"))) {
                truststore.load(in, PASSWORD);
            }
            assertThat(truststore.isCertificateEntry("acemq-dev-ca")).isTrue();
        }
    }

    @Nested
    @DisplayName("what makes them safe to lose")
    class Safety {

        @Test
        @DisplayName("every certificate carries the marker the library refuses")
        void everyCertificateIsMarked(@TempDir Path directory) throws Exception {
            DevelopmentCertificates.Result result = new DevelopmentCertificates()
                    .generate(directory, "localhost", PASSWORD, Duration.ofDays(30));

            // The marker is the whole safety story: without it on every certificate, one of
            // these reaching production would be accepted and would protect nothing.
            for (X509Certificate certificate : List.of(result.authority(), result.broker(), result.client())) {
                assertThat(certificate.getSubjectX500Principal().getName())
                        .contains(DevelopmentCertificates.MARKER);
            }
        }

        @Test
        @DisplayName("they expire, and soon")
        void theyExpire(@TempDir Path directory) throws Exception {
            DevelopmentCertificates.Result result = new DevelopmentCertificates()
                    .generate(directory, "localhost", PASSWORD, Duration.ofDays(30));

            long days = (result.expiry().getTime() - System.currentTimeMillis()) / 86_400_000L;
            assertThat(days).isBetween(28L, 31L);
        }
    }

    @Nested
    @DisplayName("the broker certificate")
    class BrokerCertificate {

        @Test
        @DisplayName("carries subject alternative names, which is what hostname checks read")
        void carriesSubjectAlternativeNames(@TempDir Path directory) throws Exception {
            DevelopmentCertificates.Result result = new DevelopmentCertificates()
                    .generate(directory, "broker.internal", PASSWORD, Duration.ofDays(30));

            Collection<List<?>> names = result.broker().getSubjectAlternativeNames();
            assertThat(names).isNotNull();
            List<Object> values = names.stream().map(entry -> entry.get(1)).map(Object.class::cast).toList();

            // Hostname verification ignores the common name entirely. A certificate without
            // these verifies as valid and is still rejected for the host it was made for,
            // which looks like a library bug and is not.
            assertThat(values).contains("broker.internal", "localhost", "127.0.0.1");
        }

        @Test
        @DisplayName("is signed by the generated authority, not self-signed")
        void isSignedByTheAuthority(@TempDir Path directory) throws Exception {
            DevelopmentCertificates.Result result = new DevelopmentCertificates()
                    .generate(directory, "localhost", PASSWORD, Duration.ofDays(30));

            assertThat(result.broker().getIssuerX500Principal())
                    .isEqualTo(result.authority().getSubjectX500Principal());
            // Verifies rather than merely naming the issuer: a chain that does not actually
            // validate would be rejected by the broker with a far less obvious message.
            result.broker().verify(result.authority().getPublicKey());
        }
    }

    @Nested
    @DisplayName("the broker configuration")
    class BrokerConfiguration {

        @Test
        @DisplayName("points at the certificates and enables the TLS listener")
        void pointsAtTheCertificates() {
            String config = new DevelopmentCertificates()
                    .rabbitMqConfiguration(Path.of("/etc/rabbitmq/certs"));

            assertThat(config).contains("listeners.ssl.default = 5671");
            assertThat(config).contains("ssl_options.certfile   = /etc/rabbitmq/certs/server.crt");
            assertThat(config).contains("ssl_options.keyfile    = /etc/rabbitmq/certs/server.key");
        }
    }
}
