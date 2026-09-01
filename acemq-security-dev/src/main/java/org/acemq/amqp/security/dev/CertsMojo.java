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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Locale;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Writes throwaway TLS certificates for local development.
 *
 * <pre>
 * mvn org.acemq:acemq-security-dev:certs -Dbroker=localhost -Dout=./certs
 * </pre>
 *
 * <p>Produces a certificate authority, a broker certificate, a client certificate, the two
 * keystores {@code Security.fromKeystore(directory)} reads, and the matching
 * {@code rabbitmq.conf}. Drop the configuration into your compose file and TLS works locally in
 * one command, which is the part that otherwise gets postponed for months.
 *
 * <p>Everything it writes is marked and short-lived, and the goal refuses to run when
 * {@code ACEMQ_ENV} says production. None of that makes a stolen development key harmless; it
 * makes one that escapes fail loudly instead of quietly protecting nothing.
 */
@Mojo(name = "certs", requiresProject = false, threadSafe = true)
public class CertsMojo extends AbstractMojo {

    /** Where to write them. */
    @Parameter(property = "out", defaultValue = "certs")
    private String out = "certs";

    /** The host the broker is reached at; becomes the common name and a subject alternative name. */
    @Parameter(property = "broker", defaultValue = "localhost")
    private String broker = "localhost";

    /**
     * Protects the generated keystores.
     *
     * <p>Six characters or more. {@code keytool} refuses to create a PKCS12 keystore with a
     * shorter password, so anything shorter produces stores that this tool can write and the
     * standard JDK tooling cannot.
     */
    @Parameter(property = "password", defaultValue = "acemq-dev")
    private String password = "acemq-dev";

    /** How many days the certificates last. Short on purpose. */
    @Parameter(property = "days", defaultValue = "30")
    private int days;

    /** Where the certificates will live inside the broker's container, for the emitted config. */
    @Parameter(property = "brokerCertificateDirectory", defaultValue = "/etc/rabbitmq/certs")
    private String brokerCertificateDirectory = "/etc/rabbitmq/certs";

    /** Set to skip writing {@code rabbitmq.conf}. */
    @Parameter(property = "skipBrokerConfig", defaultValue = "false")
    private boolean skipBrokerConfig;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        refuseInProduction();

        if (password == null || password.length() < 6) {
            throw new MojoFailureException("password must be at least six characters: keytool refuses to"
                    + " create a PKCS12 keystore with a shorter one, and a keystore the standard tooling"
                    + " cannot open is worse than no keystore at all.");
        }
        if (days < 1) {
            throw new MojoFailureException("days must be at least 1, was " + days);
        }

        Path directory = Path.of(out).toAbsolutePath().normalize();
        try {
            DevelopmentCertificates.Result result = new DevelopmentCertificates()
                    .generate(directory, broker, password.toCharArray(), Duration.ofDays(days));

            if (!skipBrokerConfig) {
                Path config = directory.resolve("rabbitmq.conf");
                Files.writeString(config,
                        new DevelopmentCertificates().rabbitMqConfiguration(Path.of(brokerCertificateDirectory)),
                        StandardCharsets.UTF_8);
                getLog().info("  rabbitmq.conf   mount at /etc/rabbitmq/rabbitmq.conf");
            }

            getLog().info("Development certificates written to " + directory);
            getLog().info("  ca.crt          the authority to trust");
            getLog().info("  server.crt/.key for the broker, valid for " + broker);
            getLog().info("  keystore.p12    this client's key pair");
            getLog().info("  truststore.p12  the authority, for Security.fromKeystore(...)");
            getLog().info("");
            getLog().info("They expire on " + result.expiry() + " and are trusted by nothing else.");
            getLog().info("AceMQ refuses them unless the policy calls allowDevelopmentCertificates().");
        } catch (IOException | GeneralSecurityException e) {
            throw new MojoExecutionException("could not write development certificates to " + directory, e);
        }
    }

    /**
     * Refuses to run where the answer should obviously be no.
     *
     * <p>Not a security control — anyone can unset an environment variable — but the mistake this
     * catches is not malice. It is a deployment script that ran a development command because
     * somebody copied a README, and this turns that into a failed build rather than a broker
     * serving certificates whose private key is in a public repository.
     */
    private void refuseInProduction() throws MojoFailureException {
        String environment = System.getenv("ACEMQ_ENV");
        if (environment != null && environment.trim().toLowerCase(Locale.ROOT).startsWith("prod")) {
            throw new MojoFailureException("ACEMQ_ENV is '" + environment + "', so this refuses to run."
                    + " These certificates are throwaway: the signing key is written next to them and is"
                    + " not a secret. Use certificates from a real authority.");
        }
    }
}
