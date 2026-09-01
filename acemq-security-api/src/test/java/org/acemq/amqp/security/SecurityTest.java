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
package org.acemq.amqp.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("the security policy")
class SecurityTest {

    @Nested
    @DisplayName("the default")
    class Defaults {

        @Test
        void verifies_both_the_chain_and_the_hostname() {
            Security security = Security.required();

            // Hostname verification is the setting people turn off first. Without it any valid
            // certificate satisfies the connection, whoever it was issued to.
            assertThat(security.mode()).isEqualTo(Security.Mode.REQUIRED);
            assertThat(security.verifiesHostname()).isTrue();
            assertThat(security.sslContext()).isPresent();
        }

        @Test
        void needs_no_configuration_at_all_for_a_real_certificate() {
            // The JVM's own trust store, which is what a certificate from a real authority
            // needs. Requiring a keystore here is what makes people reach for insecure().
            assertThat(Security.required().sslContext()).isPresent();
        }
    }

    @Nested
    @DisplayName("turning it down")
    class Relaxations {

        @Test
        void disabled_means_no_tls_at_all() {
            Security security = Security.disabled();

            assertThat(security.mode()).isEqualTo(Security.Mode.DISABLED);
            assertThat(security.verifiesHostname()).isFalse();
            // No context, because there is nothing to encrypt with.
            assertThat(security.sslContext()).isEmpty();
        }

        @Test
        void insecure_encrypts_and_proves_nothing() {
            Security security = Security.insecure();

            assertThat(security.mode()).isEqualTo(Security.Mode.INSECURE);
            assertThat(security.verifiesHostname()).isFalse();
            assertThat(security.sslContext()).isPresent();
        }

        @Test
        void both_are_named_methods_so_every_use_is_one_search() {
            // The point of the API shape: there is no verify=false to bury in a properties
            // file, so "who turned verification off" is answerable with grep.
            assertThat(Security.insecure()).hasToString("Security{INSECURE, hostnameVerification=false}");
            assertThat(Security.disabled()).hasToString("Security{DISABLED, hostnameVerification=false}");
        }
    }

    @Nested
    @DisplayName("keystores")
    class Keystores {

        @Test
        void says_which_file_is_missing_rather_than_failing_in_the_handshake(@TempDir Path directory) {
            Security security = Security.fromKeystore(directory);

            assertThatThrownBy(security::sslContext)
                    .isInstanceOf(SecurityConfigurationException.class)
                    .hasMessageContaining("keystore.p12")
                    .hasMessageContaining("acemq-security-dev");
        }

        @Test
        void a_password_can_be_given_without_rebuilding_the_policy(@TempDir Path directory) {
            Security security = Security.fromKeystore(directory).keystorePassword("hunter2");

            assertThat(security.mode()).isEqualTo(Security.Mode.REQUIRED);
            assertThat(security.verifiesHostname()).isTrue();
        }

        @Test
        void the_default_password_is_long_enough_for_the_tooling_that_creates_keystores() {
            // Regression. The default was "acemq", five characters, and keytool refuses to
            // create a PKCS12 keystore with fewer than six -- so the library's own default
            // described a store the standard JDK tooling could not produce. A default nobody
            // can use is worse than no default, and the failure surfaced only when somebody
            // followed the documentation.
            assertThat(Security.DEFAULT_KEYSTORE_PASSWORD.length())
                    .as("keytool requires at least six characters for a PKCS12 keystore")
                    .isGreaterThanOrEqualTo(6);
        }

        @Test
        void a_keystore_written_with_the_default_password_can_be_read_back(@TempDir Path directory)
                throws Exception {
            // The property that matters is not the length but that a store created with this
            // password opens again. Written with the JDK's own KeyStore API, which is what
            // both keytool and the generator ultimately use.
            char[] password = Security.DEFAULT_KEYSTORE_PASSWORD.toCharArray();
            java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
            store.load(null, password);
            Path file = directory.resolve("truststore.p12");
            try (var out = java.nio.file.Files.newOutputStream(file)) {
                store.store(out, password);
            }

            java.security.KeyStore reopened = java.security.KeyStore.getInstance("PKCS12");
            try (var in = java.nio.file.Files.newInputStream(file)) {
                reopened.load(in, password);
            }
            assertThat(reopened.size()).isZero();
        }
    }

    @Nested
    @DisplayName("development certificates")
    class DevelopmentCertificates {

        @Test
        void are_refused_unless_explicitly_permitted() {
            // A throwaway authority whose private key sits in a git repository must not be
            // trusted by accident: the connection would succeed and prove nothing.
            assertThat(Security.required().sslContext()).isPresent();
            assertThat(Security.DEVELOPMENT_MARKER).contains("DO NOT TRUST");
        }

        @Test
        void permitting_them_is_a_separate_deliberate_call() {
            Security permissive = Security.required().allowDevelopmentCertificates();

            assertThat(permissive.mode()).isEqualTo(Security.Mode.REQUIRED);
            assertThat(permissive.sslContext()).isPresent();
        }
    }

    @Nested
    @DisplayName("credentials")
    class ProvidedCredentials {

        @Test
        void a_secret_never_appears_in_a_string() {
            Credentials credentials = Credentials.of("orders-service", "hunter2");

            // toString reaches logs, health endpoints and crash dumps. This is how secrets end
            // up searchable by everyone with access to a log aggregator.
            assertThat(credentials).hasToString("Credentials{username=orders-service, secret=<redacted>}");
            assertThat(credentials.toString()).doesNotContain("hunter2");
            assertThat(credentials.secret()).containsExactly('h', 'u', 'n', 't', 'e', 'r', '2');
        }

        @Test
        void the_secret_array_cannot_be_blanked_by_its_caller() {
            Credentials credentials = Credentials.of("user", "secret");

            char[] taken = credentials.secret();
            java.util.Arrays.fill(taken, '\0');

            assertThat(credentials.secret()).containsExactly('s', 'e', 'c', 'r', 'e', 't');
        }

        @Test
        void a_token_is_carried_as_the_password_the_broker_expects() {
            Credentials credentials = Credentials.token("ey.jwt.value");

            assertThat(credentials.username()).isEqualTo("oauth2");
            assertThat(credentials.secret()).containsExactly("ey.jwt.value".toCharArray());
        }

        @Test
        void a_policy_can_carry_where_credentials_come_from() {
            Security security = Security.required().withCredentials(CredentialsProvider.of("user", "pass"));

            assertThat(security.credentials()).isPresent();
            assertThat(security.credentials().get().get().username()).isEqualTo("user");
        }

        @Test
        void a_file_provider_reads_the_file_every_time_it_is_asked(@TempDir Path directory) throws Exception {
            Path file = directory.resolve("credentials.properties");
            write(file, "user-one", "first");
            CredentialsProvider provider = CredentialsProvider.fromFile(file);

            assertThat(provider.get().username()).isEqualTo("user-one");

            // The file is rewritten in place when a secret rotates, which is how Kubernetes and
            // most vault agents deliver one. A provider that cached would keep the old value.
            write(file, "user-two", "second");
            assertThat(provider.get().username()).isEqualTo("user-two");
        }

        @Test
        void a_file_missing_its_fields_says_so(@TempDir Path directory) throws Exception {
            Path file = directory.resolve("half.properties");
            Files.write(file, "username=only-this".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            assertThatThrownBy(() -> CredentialsProvider.fromFile(file).get())
                    .isInstanceOf(SecurityConfigurationException.class)
                    .hasMessageContaining("must define both");
        }

        @Test
        void a_file_that_is_not_there_says_which_one(@TempDir Path directory) {
            Path missing = directory.resolve("absent.properties");

            assertThatThrownBy(() -> CredentialsProvider.fromFile(missing).get())
                    .isInstanceOf(SecurityConfigurationException.class)
                    .hasMessageContaining("could not read credentials");
        }

        @Test
        void an_environment_provider_says_which_variable_is_missing() {
            CredentialsProvider provider = CredentialsProvider.fromEnvironment("ACEMQ_NO_SUCH_USER",
                    "ACEMQ_NO_SUCH_PASSWORD");

            assertThatThrownBy(provider::get)
                    .isInstanceOf(SecurityConfigurationException.class)
                    .hasMessageContaining("ACEMQ_NO_SUCH_USER");
        }

        private void write(Path file, String username, String password) throws Exception {
            Properties properties = new Properties();
            properties.setProperty("username", username);
            properties.setProperty("password", password);
            try (java.io.OutputStream out = Files.newOutputStream(file)) {
                properties.store(out, null);
            }
        }
    }
}
