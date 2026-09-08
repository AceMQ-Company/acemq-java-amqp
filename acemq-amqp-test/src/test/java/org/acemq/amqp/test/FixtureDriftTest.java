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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.acemq.amqp.core.ContractFixtures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regenerates the cross-language fixtures and fails when they no longer match what is committed.
 *
 * <p>This is the whole point of generating them. Four other libraries carry a copy of these files
 * and assert against it, so a change to the retry arithmetic, the queue naming or the declared
 * topology is a change to their tests as much as to this one. Without this test that change is
 * invisible until a customer finds it; with it, the build goes red on the commit that made it,
 * with a diff naming exactly what moved.
 *
 * <p>When a change is deliberate, regenerate with:
 *
 * <pre>
 * mvn -pl acemq-amqp-test test -Dtest=FixtureDriftTest -Dacemq.fixtures.write=true
 * </pre>
 *
 * <p>and then carry the new bytes to acemq-go-amqp, acemq-dotnet-amqp, acemq-python-amqp and
 * acemq-ruby-amqp in the same change. A fixture updated in one repository and not the other four
 * is worse than no fixture at all, because it looks like agreement.
 */
@DisplayName("the cross-language fixtures")
class FixtureDriftTest {

    /** Where the committed copies live, relative to the module Surefire runs in. */
    private static final Path FIXTURES = Paths.get("src", "test", "resources", "fixtures");

    /** Set to regenerate rather than compare. */
    private static final String WRITE = "acemq.fixtures.write";

    /**
     * The values of the envelope fixture that cannot be reproduced, and are not meant to be.
     *
     * <p>The minimal case is the case where the caller supplies nothing, so its identifier is a
     * fresh UUID, its first-seen is the clock and its origin carries the hostname of whichever
     * machine ran the generator — the committed copy still says {@code acemq@Kenshi.local}. The
     * four ports read this file as input to their own round trip rather than as literal
     * expectations, so those values are arbitrary to them. They are masked here and their shape
     * is asserted separately, which is the honest version of "byte-identical": every byte that
     * can be reproduced is compared, and the four that cannot are checked for being the right
     * kind of thing.
     */
    private static final List<String> UNREPRODUCIBLE = Arrays.asList("messageId", "x-acemq-id", "x-acemq-correlation",
            "x-acemq-first-seen", "x-acemq-origin");

    private static final Pattern CASE_NAME = Pattern.compile("^\\s*\"case\": \"([^\"]*)\",$");

    private static final Pattern UUID = Pattern
            .compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    @AfterEach
    void tearDown() {
        InMemoryTransport.reset();
    }

    @Test
    @Timeout(60)
    @DisplayName("contract-fixtures.json is exactly what the library produces today")
    void the_contract_fixtures_have_not_drifted() throws IOException {
        Path committed = FIXTURES.resolve("contract-fixtures.json");
        String regenerated = ContractFixtures.generate();

        if (Boolean.getBoolean(WRITE)) {
            write(committed, regenerated);
            return;
        }

        assertThat(committed).as("the committed contract fixtures").exists();
        assertThat(read(committed))
                .as("contract-fixtures.json differs from what this library produces now. Either the change"
                        + " to the retry arithmetic, the naming or the topology was not intended, or it was"
                        + " and the four ports need the new bytes too — see this class's javadoc")
                .isEqualTo(regenerated);
    }

    @Test
    @Timeout(60)
    @DisplayName("envelope-fixtures.json is exactly what the library produces today, bar the clock and the host")
    void the_envelope_fixtures_have_not_drifted() throws IOException {
        Path committed = FIXTURES.resolve("envelope-fixtures.json");
        String regenerated = EnvelopeFixtures.generate();

        assertThat(committed).as("the committed envelope fixtures").exists();
        String before = mask(read(committed));
        String after = mask(regenerated);

        if (Boolean.getBoolean(WRITE)) {
            // Only when the contract actually moved. Rewriting it otherwise would roll a new
            // UUID and stamp a new hostname into a file four repositories hold byte-identical
            // copies of, which is a five-repository change for no change at all.
            if (!before.equals(after)) {
                write(committed, regenerated);
            }
            return;
        }

        assertThat(before)
                .as("envelope-fixtures.json differs from what this library puts on the wire now. Four"
                        + " repositories carry a byte-identical copy of it")
                .isEqualTo(after);
    }

    @Test
    @Timeout(60)
    @DisplayName("the values that cannot be reproduced are still the right kind of thing")
    void the_masked_values_are_what_the_contract_says_they_are() {
        String regenerated = EnvelopeFixtures.generate();
        String minimal = caseBlock(regenerated, "minimal");

        String id = value(minimal, "x-acemq-id");
        assertThat(id).as("the identifier the library invents").matches(UUID);
        assertThat(value(minimal, "messageId"))
                .as("the AMQP message id, which carries the envelope id")
                .isEqualTo(id);
        assertThat(value(minimal, "x-acemq-correlation"))
                .as("a message that starts a conversation correlates with itself")
                .isEqualTo(id);
        assertThat(value(minimal, "x-acemq-origin"))
                .as("the default origin is the library and the host")
                .matches("^acemq@.+");

        long firstSeen = Long.parseLong(value(minimal, "x-acemq-first-seen"));
        assertThat(Instant.ofEpochMilli(firstSeen))
                .as("first-seen is epoch milliseconds, not seconds and not an ISO string")
                .isBetween(Instant.now().minus(Duration.ofMinutes(5)), Instant.now().plus(Duration.ofMinutes(5)));

        // The populated case supplies all of them, so none of it is masked and all of it is
        // compared byte for byte by the test above. Named here so that a future reader does not
        // conclude the masking is wider than it is.
        String populated = caseBlock(regenerated, "populated");
        assertThat(value(populated, "x-acemq-id")).isEqualTo("11111111-2222-3333-4444-555555555555");
        assertThat(value(populated, "x-acemq-first-seen")).isEqualTo("1767323045678");
        assertThat(value(populated, "x-acemq-origin")).isEqualTo("orders@host-7");
    }

    // ---------------------------------------------------------------- helpers

    /** Blanks the unreproducible values of the minimal case, and only of the minimal case. */
    private static String mask(String json) {
        String[] lines = json.split("\n", -1);
        String current = "";
        for (int i = 0; i < lines.length; i++) {
            Matcher name = CASE_NAME.matcher(lines[i]);
            if (name.matches()) {
                current = name.group(1);
            }
            if (!"minimal".equals(current)) {
                continue;
            }
            for (String key : UNREPRODUCIBLE) {
                // Keeps the indentation, the key and whatever trailing whitespace and comma the
                // generator emitted, so the comparison still fails on a formatting change.
                lines[i] = lines[i].replaceAll(
                        "^(\\s*\"" + Pattern.quote(key) + "\": )(.*?)(\\s*,?)$", "$1<varies>$3");
            }
        }
        return String.join("\n", lines);
    }

    private static String caseBlock(String json, String name) {
        int start = json.indexOf("\"case\": \"" + name + "\"");
        assertThat(start).as("the " + name + " case is in the fixture").isNotNegative();
        int end = json.indexOf("\"case\": \"", start + 1);
        return end < 0 ? json.substring(start) : json.substring(start, end);
    }

    private static String value(String block, String key) {
        Matcher matcher = Pattern.compile("\"" + Pattern.quote(key) + "\": (\"([^\"]*)\"|[^,\\s]+)")
                .matcher(block);
        assertThat(matcher.find()).as(key + " is present").isTrue();
        return matcher.group(2) == null ? matcher.group(1) : matcher.group(2);
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static void write(Path path, String content) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
        System.out.println("wrote " + path.toAbsolutePath());
    }
}
