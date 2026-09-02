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
package org.acemq.amqp.patterns;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ClaimCheckStore;

/**
 * A claim-check store on a filesystem.
 *
 * <p>Useful where the filesystem is shared and durable — an NFS mount, a persistent volume — and
 * the honest middle ground between a map and object storage. On a container's local disk it is
 * the in-memory store with extra steps: the consumer is on another host and finds nothing.
 *
 * <p>Object storage is the usual right answer, and a store in front of S3 or Azure Blob Storage
 * is three short methods. This one exists because "write it to the mount everything already has"
 * is a real deployment and not a bad one.
 *
 * <h2>Writes are atomic</h2>
 *
 * <p>The payload is written to a temporary file and moved into place. Without that, a consumer
 * fast enough to read the key before the writer finished gets a truncated payload and a parse
 * error somewhere unhelpful — and messaging is exactly the arrangement that makes a consumer
 * that fast normal rather than unlikely.
 */
public final class FilesystemClaimCheckStore implements ClaimCheckStore {

    /**
     * A key reaches the filesystem as a path segment, so it is checked rather than trusted.
     * Every key this store issues is a UUID; one arriving from a message is whatever a
     * publisher put there, and {@code ../../etc/passwd} is a key too.
     */
    private static final Pattern SAFE_KEY = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private final Path directory;

    /**
     * @param directory where payloads are written; created if it is not there
     */
    public FilesystemClaimCheckStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new AceMqException("could not create the claim-check directory " + directory, e);
        }
    }

    @Override
    public String put(byte[] content) {
        String key = UUID.randomUUID().toString();
        Path target = directory.resolve(key);
        try {
            Path staging = Files.createTempFile(directory, key, ".partial");
            Files.write(staging, content);
            // Moved into place, so a reader sees the whole payload or no payload. ATOMIC_MOVE
            // where the filesystem offers it; the plain move is still far better than writing
            // in place, and some network filesystems refuse the atomic one.
            try {
                Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(staging, target, StandardCopyOption.REPLACE_EXISTING);
            }
            return key;
        } catch (IOException e) {
            throw new AceMqException("could not store a claim-check payload in " + directory, e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        Path path = pathFor(key);
        try {
            return Optional.of(Files.readAllBytes(path));
        } catch (NoSuchFileException e) {
            // Absent rather than failed: a key the store no longer holds is a retention answer,
            // and the codec turns it into a message that explains itself.
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the claim-check payload " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            Files.deleteIfExists(pathFor(key));
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete the claim-check payload " + key, e);
        }
    }

    private Path pathFor(String key) {
        if (!SAFE_KEY.matcher(Objects.requireNonNull(key, "key")).matches()) {
            throw new AceMqException("'" + key + "' is not a key this store issued. A key becomes a"
                    + " path segment, so one arriving from a message is checked rather than trusted.");
        }
        return directory.resolve(key);
    }

    @Override
    public String toString() {
        return "FilesystemClaimCheckStore{" + directory + "}";
    }
}
