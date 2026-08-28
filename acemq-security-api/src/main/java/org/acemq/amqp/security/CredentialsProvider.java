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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Supplies the credentials to connect with, every time a connection is made.
 *
 * <p><strong>Consulted on every connection attempt, not once at start-up.</strong> That is the
 * whole reason this is an interface rather than two strings, and it is the detail most clients
 * get wrong. A token has an expiry. A client that reads one at start-up works beautifully for
 * fifty-nine minutes and then cannot reconnect — at three in the morning, looking exactly like a
 * broker fault, because the broker is the thing rejecting it.
 *
 * <p>Implementations must therefore be cheap to call and safe from many threads. Fetching from a
 * vault belongs behind a short cache inside the implementation, not in a field read once.
 */
@FunctionalInterface
public interface CredentialsProvider {

    /**
     * @return the credentials to use for the connection about to be made
     * @throws SecurityConfigurationException if they cannot be obtained
     */
    Credentials get();

    /**
     * Fixed credentials.
     *
     * <p>Fine for a test and for a password that genuinely never rotates. Anything read from a
     * vault or an identity provider should not use this, because it will be read once.
     *
     * @param username the user
     * @param password its password
     * @return a provider returning them
     */
    static CredentialsProvider of(String username, String password) {
        Credentials fixed = Credentials.of(username, password);
        return () -> fixed;
    }

    /**
     * Credentials from environment variables, re-read each time.
     *
     * <p>Re-reading matters: a sidecar that rotates a secret by rewriting the environment of a
     * restarted process is common, and so is one that rewrites a mounted file.
     *
     * @param usernameVariable name of the variable holding the user
     * @param passwordVariable name of the variable holding the password
     * @return a provider reading them on every call
     */
    static CredentialsProvider fromEnvironment(String usernameVariable, String passwordVariable) {
        Objects.requireNonNull(usernameVariable, "usernameVariable");
        Objects.requireNonNull(passwordVariable, "passwordVariable");
        return () -> {
            String username = System.getenv(usernameVariable);
            String password = System.getenv(passwordVariable);
            if (username == null || password == null) {
                throw new SecurityConfigurationException("the environment does not define "
                        + (username == null ? usernameVariable : passwordVariable)
                        + ", and credentials were asked for. Set it, or configure a different provider.");
            }
            return Credentials.of(username, password);
        };
    }

    /**
     * Credentials from a properties file, re-read each time.
     *
     * <p>For a secret mounted as a file, which is how Kubernetes and most vault agents deliver
     * one. Re-reading is the point: the file is rewritten in place when the secret rotates, and
     * a provider that cached it would keep presenting the old one.
     *
     * @param file a properties file holding {@code username} and {@code password}
     * @return a provider reading it on every call
     */
    static CredentialsProvider fromFile(Path file) {
        Objects.requireNonNull(file, "file");
        return () -> {
            Properties properties = new Properties();
            try (java.io.InputStream in = Files.newInputStream(file)) {
                properties.load(in);
            } catch (IOException e) {
                throw new SecurityConfigurationException("could not read credentials from " + file, e);
            }
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            if (username == null || password == null) {
                throw new SecurityConfigurationException(
                        file + " must define both username and password, and does not");
            }
            return Credentials.of(username, password);
        };
    }
}
