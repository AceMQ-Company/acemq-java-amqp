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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Generates a throwaway certificate authority and the certificates a local broker and client
 * need, so that running with TLS on a developer's machine is one command rather than an
 * afternoon with {@code openssl}.
 *
 * <p>Everything produced here carries {@code ACEMQ DEVELOPMENT ONLY - DO NOT TRUST} in its
 * subject, and AceMQ refuses such a certificate unless the policy explicitly calls
 * {@code allowDevelopmentCertificates()}. That is the point: a self-signed authority that drifts
 * into production is <em>worse</em> than no encryption, because everything looks protected and
 * nothing is verified. These fail closed instead.
 *
 * <p>Certificates are short-lived by default. A development certificate that never expires is one
 * that outlives the reason it was created.
 */
public final class DevelopmentCertificates {

    /** Written into every subject, and refused by the library unless explicitly allowed. */
    public static final String MARKER = "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST";

    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final int KEY_SIZE = 4096;

    private final SecureRandom random = new SecureRandom();

    /**
     * Writes a CA, a broker certificate, a client certificate and the two keystores AceMQ reads.
     *
     * @param directory where to write them; created if absent
     * @param brokerHost the host the broker will be reached at, which becomes the certificate's
     *     common name and a subject alternative name
     * @param password protects the generated keystores
     * @param validity how long the certificates are good for
     * @return what was written
     * @throws IOException if the files cannot be written
     * @throws GeneralSecurityException if the certificates cannot be generated
     */
    public Result generate(Path directory, String brokerHost, char[] password, Duration validity)
            throws IOException, GeneralSecurityException {
        Files.createDirectories(directory);

        Instant now = Instant.now();
        // Backdated a little. A machine whose clock is a few minutes behind the one that
        // generated these would otherwise reject them as not yet valid, which is a confusing
        // way to spend a morning.
        Date from = Date.from(now.minus(Duration.ofHours(1)));
        Date until = Date.from(now.plus(validity));

        KeyPair authorityKeys = keyPair();
        X509Certificate authority = certificate(
                new X500Name("CN=AceMQ development CA, OU=" + MARKER),
                authorityKeys,
                new X500Name("CN=AceMQ development CA, OU=" + MARKER),
                authorityKeys.getPrivate(),
                from,
                until,
                true,
                null);

        KeyPair brokerKeys = keyPair();
        X509Certificate broker = certificate(
                new X500Name("CN=" + brokerHost + ", OU=" + MARKER),
                brokerKeys,
                new X500Name("CN=AceMQ development CA, OU=" + MARKER),
                authorityKeys.getPrivate(),
                from,
                until,
                false,
                // Hostname verification reads the subject alternative name and ignores the
                // common name entirely. Without these the certificate verifies as valid and is
                // still rejected for the host, which looks like a library bug and is not.
                subjectAlternativeNames(brokerHost));

        KeyPair clientKeys = keyPair();
        X509Certificate client = certificate(
                new X500Name("CN=acemq-client, OU=" + MARKER),
                clientKeys,
                new X500Name("CN=AceMQ development CA, OU=" + MARKER),
                authorityKeys.getPrivate(),
                from,
                until,
                false,
                null);

        writePem(directory.resolve("ca.crt"), authority);
        writePem(directory.resolve("ca.key"), authorityKeys.getPrivate());
        writePem(directory.resolve("server.crt"), broker);
        writePem(directory.resolve("server.key"), brokerKeys.getPrivate());
        writePem(directory.resolve("client.crt"), client);
        writePem(directory.resolve("client.key"), clientKeys.getPrivate());

        // The two stores AceMQ's Security.fromKeystore(directory) looks for by name.
        writeKeystore(directory.resolve("keystore.p12"), password, clientKeys.getPrivate(),
                new X509Certificate[]{client, authority});
        writeTruststore(directory.resolve("truststore.p12"), password, authority);

        restrictPrivateKeys(directory);
        return new Result(directory, authority, broker, client, until);
    }

    /** @return a {@code rabbitmq.conf} that serves TLS with what {@link #generate} wrote */
    public String rabbitMqConfiguration(Path certificateDirectoryInContainer) {
        String dir = certificateDirectoryInContainer.toString();
        return "# Generated by acemq-security-dev. Development only.\n"
                + "listeners.tcp.default = 5672\n"
                + "listeners.ssl.default = 5671\n"
                + "\n"
                + "ssl_options.cacertfile = " + dir + "/ca.crt\n"
                + "ssl_options.certfile   = " + dir + "/server.crt\n"
                + "ssl_options.keyfile    = " + dir + "/server.key\n"
                + "\n"
                + "# The broker does not ask the client for a certificate. Mutual TLS sets these\n"
                + "# to verify_peer and true, and the generated client keystore then does real work.\n"
                + "ssl_options.verify               = verify_none\n"
                + "ssl_options.fail_if_no_peer_cert = false\n";
    }

    private KeyPair keyPair() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(KEY_SIZE, random);
        return generator.generateKeyPair();
    }

    private X509Certificate certificate(
            X500Name subject,
            KeyPair subjectKeys,
            X500Name issuer,
            PrivateKey issuerKey,
            Date from,
            Date until,
            boolean authority,
            @org.jspecify.annotations.Nullable GeneralNames alternativeNames)
            throws GeneralSecurityException {
        try {
            JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                    issuer,
                    new BigInteger(64, random),
                    from,
                    until,
                    subject,
                    subjectKeys.getPublic());

            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(authority));
            if (authority) {
                builder.addExtension(Extension.keyUsage, true,
                        new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
            } else {
                builder.addExtension(Extension.keyUsage, true,
                        new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
                builder.addExtension(Extension.extendedKeyUsage, false,
                        new ExtendedKeyUsage(new KeyPurposeId[]{
                                KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth
                        }));
            }
            if (alternativeNames != null) {
                builder.addExtension(Extension.subjectAlternativeName, false, alternativeNames);
            }

            return new JcaX509CertificateConverter().getCertificate(
                    builder.build(new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(issuerKey)));
        } catch (OperatorCreationException | IOException e) {
            throw new GeneralSecurityException("could not generate a development certificate", e);
        }
    }

    private static GeneralNames subjectAlternativeNames(String brokerHost) {
        // localhost and the loopback address are always included: a broker in a container is
        // reached at one of them however it is named on the certificate.
        return new GeneralNames(new GeneralName[]{
                new GeneralName(GeneralName.dNSName, brokerHost),
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.dNSName, "rabbitmq"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        });
    }

    private static void writePem(Path file, Object encodable) throws IOException {
        try (Writer out = new OutputStreamWriter(Files.newOutputStream(file), StandardCharsets.UTF_8);
                JcaPEMWriter pem = new JcaPEMWriter(out)) {
            pem.writeObject(encodable);
        }
    }

    private static void writeKeystore(Path file, char[] password, PrivateKey key, X509Certificate[] chain)
            throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, password);
        store.setKeyEntry("acemq-client", key, password, chain);
        try (var out = Files.newOutputStream(file)) {
            store.store(out, password);
        }
    }

    private static void writeTruststore(Path file, char[] password, X509Certificate authority)
            throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, password);
        store.setCertificateEntry("acemq-dev-ca", authority);
        try (var out = Files.newOutputStream(file)) {
            store.store(out, password);
        }
    }

    /**
     * Takes the group and world permissions off the private keys.
     *
     * <p>Best effort: filesystems that do not carry POSIX permissions simply skip it. Worth doing
     * where it is supported, because a world-readable private key is a habit that survives the
     * move from a laptop to somewhere it matters.
     */
    private static void restrictPrivateKeys(Path directory) {
        for (String name : List.of("ca.key", "server.key", "client.key", "keystore.p12", "truststore.p12")) {
            Path file = directory.resolve(name);
            try {
                Files.setPosixFilePermissions(file,
                        java.util.Set.of(java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                                java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
            } catch (UnsupportedOperationException | IOException e) {
                // Windows, or a filesystem without POSIX permissions. Not worth failing over.
            }
        }
    }

    /** What {@link #generate} wrote. */
    public static final class Result {

        private final Path directory;
        private final X509Certificate authority;
        private final X509Certificate broker;
        private final X509Certificate client;
        private final Date expiry;

        Result(Path directory, X509Certificate authority, X509Certificate broker, X509Certificate client,
                Date expiry) {
            this.directory = directory;
            this.authority = authority;
            this.broker = broker;
            this.client = client;
            this.expiry = new Date(expiry.getTime());
        }

        public Path directory() {
            return directory;
        }

        public X509Certificate authority() {
            return authority;
        }

        public X509Certificate broker() {
            return broker;
        }

        public X509Certificate client() {
            return client;
        }

        /** @return when these stop working, which is deliberately soon */
        public Date expiry() {
            return new Date(expiry.getTime());
        }
    }
}
