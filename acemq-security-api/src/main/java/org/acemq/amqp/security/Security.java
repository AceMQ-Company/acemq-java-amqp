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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Objects;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.jspecify.annotations.Nullable;

/**
 * How a connection is protected.
 *
 * <p>Three states, and the names are the documentation:
 *
 * <pre>{@code
 * Security.required()                    // verify the chain and the hostname. The default.
 * Security.fromKeystore(Path.of("certs")) // the same, with a client certificate for mutual TLS
 * Security.disabled()                    // plaintext. For tests.
 * Security.insecure()                    // TLS with no verification. Almost always wrong.
 * }</pre>
 *
 * <p>{@link #insecure()} exists because sometimes it is genuinely needed, and because a library
 * that omits it gets a {@code verify=false} added to a properties file by somebody in a hurry.
 * Making it a named method keeps it greppable: one search finds every place it was used, which a
 * boolean buried in configuration does not.
 *
 * <p>Hostname verification is the setting people turn off first and should turn off last.
 * Without it any valid certificate satisfies the connection, whoever it was issued to, which
 * reduces TLS to an encrypted conversation with whoever happened to answer.
 */
public final class Security {

    /** Subject the development certificate generator stamps, so this library can refuse it. */
    public static final String DEVELOPMENT_MARKER = "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST";

    /**
     * What {@link #fromKeystore} assumes when nothing says otherwise, and what
     * {@code acemq-security-dev} writes.
     *
     * <p>Six characters, because {@code keytool} refuses to create a PKCS12 keystore with a
     * shorter password. The previous default was five, which meant the library's own default
     * described a keystore the standard JDK tooling could not produce — a default nobody could
     * use is worse than no default.
     *
     * <p>It protects a store on a developer's machine and is not a secret. Anything real passes
     * {@link #keystorePassword(String)} with a value from a secret store.
     */
    public static final String DEFAULT_KEYSTORE_PASSWORD = "acemq-dev";

    /** What a connection does about transport security. */
    public enum Mode {
        /** Encrypt, verify the chain, verify the hostname. */
        REQUIRED,
        /** Encrypt and verify nothing. */
        INSECURE,
        /** No encryption at all. */
        DISABLED
    }

    private final Mode mode;
    private final @Nullable Path keystoreDirectory;
    private final char[] keystorePassword;
    private final boolean allowDevelopmentCertificates;
    private final @Nullable CredentialsProvider credentials;

    private Security(
            Mode mode,
            @Nullable Path keystoreDirectory,
            char[] keystorePassword,
            boolean allowDevelopmentCertificates,
            @Nullable CredentialsProvider credentials) {
        this.mode = mode;
        this.keystoreDirectory = keystoreDirectory;
        this.keystorePassword = keystorePassword.clone();
        this.allowDevelopmentCertificates = allowDevelopmentCertificates;
        this.credentials = credentials;
    }

    /** @return TLS with the chain and the hostname verified, using the JVM's trust store */
    public static Security required() {
        return new Security(Mode.REQUIRED, null, DEFAULT_KEYSTORE_PASSWORD.toCharArray(), false, null);
    }

    /**
     * TLS using the keystores in a directory, as written by the development generator or by
     * your own tooling: {@code keystore.p12} for this client, {@code truststore.p12} for the
     * certificates it will accept.
     *
     * @param directory where the two stores live
     * @return the policy
     */
    public static Security fromKeystore(Path directory) {
        return new Security(
                Mode.REQUIRED,
                Objects.requireNonNull(directory, "directory"),
                DEFAULT_KEYSTORE_PASSWORD.toCharArray(),
                false,
                null);
    }

    /** @return plaintext, for tests and for a broker on this machine */
    public static Security disabled() {
        return new Security(Mode.DISABLED, null, new char[0], false, null);
    }

    /**
     * TLS with no verification at all.
     *
     * <p>Encrypts the conversation and proves nothing about who is on the other end, which stops
     * a passive eavesdropper and does nothing about an active one. Named so that finding every
     * use of it is one search.
     *
     * @return the policy
     */
    public static Security insecure() {
        return new Security(Mode.INSECURE, null, new char[0], false, null);
    }

    /**
     * @param password the password protecting the keystores
     * @return a copy of this policy using it
     */
    public Security keystorePassword(String password) {
        return new Security(
                mode,
                keystoreDirectory,
                Objects.requireNonNull(password, "password").toCharArray(),
                allowDevelopmentCertificates,
                credentials);
    }

    /**
     * Permits certificates the development generator produced.
     *
     * <p>Needed on a developer's machine and nowhere else. Without it, a certificate carrying
     * {@link #DEVELOPMENT_MARKER} is refused however the trust store was configured — because a
     * throwaway certificate authority that drifts into production is worse than no encryption:
     * everything looks protected and nothing is.
     *
     * @return a copy of this policy permitting them
     */
    public Security allowDevelopmentCertificates() {
        return new Security(mode, keystoreDirectory, keystorePassword, true, credentials);
    }

    /**
     * @param provider where credentials come from, consulted on every connection
     * @return a copy of this policy using it
     */
    public Security withCredentials(CredentialsProvider provider) {
        return new Security(
                mode,
                keystoreDirectory,
                keystorePassword,
                allowDevelopmentCertificates,
                Objects.requireNonNull(provider, "provider"));
    }

    /** @return what this policy does about transport security */
    public Mode mode() {
        return mode;
    }

    /** @return whether the hostname on the certificate must match the host connected to */
    public boolean verifiesHostname() {
        return mode == Mode.REQUIRED;
    }

    /** @return where credentials come from, if this policy carries a provider */
    public java.util.Optional<CredentialsProvider> credentials() {
        return java.util.Optional.ofNullable(credentials);
    }

    /**
     * Builds the context to connect with.
     *
     * @return an {@link SSLContext}, or empty when this policy is {@link Mode#DISABLED}
     * @throws SecurityConfigurationException if the keystores cannot be read
     */
    public java.util.Optional<SSLContext> sslContext() {
        if (mode == Mode.DISABLED) {
            return java.util.Optional.empty();
        }
        try {
            SSLContext context = SSLContext.getInstance("TLSv1.2");
            if (mode == Mode.INSECURE) {
                context.init(null, new TrustManager[]{new TrustEverything()}, null);
                return java.util.Optional.of(context);
            }
            context.init(keyManagers(), trustManagers(), null);
            return java.util.Optional.of(context);
        } catch (GeneralSecurityException | IOException e) {
            throw new SecurityConfigurationException("could not build the TLS context", e);
        }
    }

    private javax.net.ssl.KeyManager[] keyManagers() throws GeneralSecurityException, IOException {
        if (keystoreDirectory == null) {
            return new javax.net.ssl.KeyManager[0];
        }
        KeyStore keystore = load(keystoreDirectory.resolve("keystore.p12"));
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(keystore, keystorePassword);
        return factory.getKeyManagers();
    }

    private TrustManager[] trustManagers() throws GeneralSecurityException, IOException {
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        // A null keystore means the JVM's own trust store, which is what a real certificate
        // needs and what makes required() work with no configuration at all.
        factory.init(keystoreDirectory == null ? null : load(keystoreDirectory.resolve("truststore.p12")));

        TrustManager[] delegates = factory.getTrustManagers();
        if (allowDevelopmentCertificates) {
            return delegates;
        }
        for (int i = 0; i < delegates.length; i++) {
            if (delegates[i] instanceof X509TrustManager) {
                delegates[i] = new RefuseDevelopmentCertificates((X509TrustManager) delegates[i]);
            }
        }
        return delegates;
    }

    private KeyStore load(Path path) throws GeneralSecurityException, IOException {
        if (!Files.exists(path)) {
            throw new SecurityConfigurationException(path + " does not exist. Generate a development pair with"
                    + " acemq-security-dev, or point fromKeystore at the directory holding keystore.p12 and"
                    + " truststore.p12.");
        }
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(path)) {
            keystore.load(in, keystorePassword);
        }
        return keystore;
    }

    @Override
    public String toString() {
        return "Security{" + mode + ", hostnameVerification=" + verifiesHostname() + "}";
    }

    /** Accepts anything. Only reachable through {@link #insecure()}. */
    private static final class TrustEverything implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // Deliberately empty: this is what insecure() means.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // Deliberately empty.
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    /**
     * Refuses a chain containing a development certificate.
     *
     * <p>The safety net under the generator. Trusting one of those in production would mean
     * trusting a certificate authority whose private key is in a git repository, and the failure
     * is silent because the connection succeeds.
     */
    private static final class RefuseDevelopmentCertificates implements X509TrustManager {

        private final X509TrustManager delegate;

        RefuseDevelopmentCertificates(X509TrustManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            refuseDevelopment(chain);
            delegate.checkClientTrusted(chain, authType);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            refuseDevelopment(chain);
            delegate.checkServerTrusted(chain, authType);
        }

        private void refuseDevelopment(X509Certificate[] chain) throws java.security.cert.CertificateException {
            for (X509Certificate certificate : chain) {
                if (certificate.getSubjectX500Principal().getName().contains(DEVELOPMENT_MARKER)
                        || certificate.getIssuerX500Principal().getName().contains(DEVELOPMENT_MARKER)) {
                    throw new java.security.cert.CertificateException("this chain contains a development"
                            + " certificate, whose signing key is not a secret. Use a real certificate, or call"
                            + " allowDevelopmentCertificates() if this really is a developer's machine.");
                }
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }
    }

    /** @return the certificates a keystore holds, for tests and diagnostics */
    static java.util.List<Certificate> certificatesIn(KeyStore keystore) throws GeneralSecurityException {
        java.util.List<Certificate> found = new java.util.ArrayList<>();
        for (String alias : java.util.Collections.list(keystore.aliases())) {
            Certificate certificate = keystore.getCertificate(alias);
            if (certificate != null) {
                found.add(certificate);
            }
        }
        return found;
    }
}
