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
package org.acemq.amqp.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.CodecProvider;

/**
 * Finds the codecs on the classpath and names the ones used by default.
 *
 * <p>Formats are discovered rather than listed, for the same reason transports are: the core
 * cannot depend on an XML library, an Avro library and a Protobuf library at once, and should not
 * try. Each format is a module that announces itself, and asking for one that is not there
 * produces an error naming the artifact to add rather than a missing class.
 *
 * <p>The defaults are deliberately asymmetric. Publishing uses JSON and nothing else, because a
 * queue carrying two formats at once is a queue no consumer can be written against, and the
 * format on the wire is a contract with services that are not being redeployed. Consuming uses
 * every codec on the classpath, because a consumer that refuses a message it could have read has
 * turned somebody else's deployment into its own outage.
 *
 * <p>Write one format, read all of them. That asymmetry is what makes changing format two
 * ordinary releases instead of a flag day.
 */
public final class Codecs {

    /** The format published in unless the application says otherwise. */
    public static final String DEFAULT_FORMAT = "json";

    private Codecs() {
        throw new AssertionError("Codecs is a static holder and must not be instantiated");
    }

    /**
     * @param name short format name, such as {@code json} or {@code xml}
     * @return a codec for that format
     * @throws AceMqException if no module on the classpath provides it
     */
    public static Codec byName(String name) {
        for (CodecProvider provider : providers()) {
            if (provider.name().equalsIgnoreCase(name)) {
                return provider.create();
            }
        }
        String asked = name.toLowerCase(java.util.Locale.ROOT);
        if ("avro".equals(asked) || "protobuf".equals(asked)) {
            // Not an oversight, and worth explaining where somebody will actually read it. Both
            // formats need a schema before a codec exists at all, so there is nothing a method
            // taking no arguments could build.
            throw new AceMqException(asked + " is not chosen by name, because a " + asked + " codec cannot be"
                    + " built without a schema: the bytes carry no description of themselves and a reader has"
                    + " to be told what they are. Use publisher.as(AvroCodec.of(schema)) or"
                    + " publisher.as(ProtobufCodec.of(YourMessage.parser())) instead, from"
                    + " org.acemq:acemq-amqp-codec-" + asked + ".");
        }
        throw new AceMqException("no codec named '" + name + "'. Formats available on the classpath: "
                + available() + ". Add the module for the format you want, for example"
                + " org.acemq:acemq-amqp-codec-" + asked + ".");
    }

    /**
     * @return the codec used to publish when the application has not chosen one
     */
    public static Codec forPublishing() {
        return byName(DEFAULT_FORMAT);
    }

    /**
     * Every codec on the classpath, in the order they should be tried.
     *
     * <p>What a consumer reads with. A message that names its format goes to the codec that
     * claims it; a message that names none is offered to each in turn, which is the only useful
     * answer when a sender has told us nothing.
     *
     * @return a codec that reads any known format and writes the default one
     */
    public static Codec forConsuming() {
        List<Codec> all = new ArrayList<>();
        // The default format goes first so that this codec writes what forPublishing writes,
        // for the case where one codec is used on both sides of a connection.
        all.add(forPublishing());
        for (CodecProvider provider : providers()) {
            if (!provider.name().equalsIgnoreCase(DEFAULT_FORMAT)) {
                all.add(provider.create());
            }
        }
        return CompositeCodec.of(all.toArray(new Codec[0]));
    }

    /** @return the format names available on this classpath, in the order they are tried */
    public static List<String> names() {
        return providers().stream().map(CodecProvider::name).collect(Collectors.toList());
    }

    private static List<CodecProvider> providers() {
        List<CodecProvider> found = new ArrayList<>();
        for (CodecProvider provider : ServiceLoader.load(CodecProvider.class, Codecs.class.getClassLoader())) {
            found.add(provider);
        }
        if (found.isEmpty()) {
            // Only reachable when the jars have been repackaged in a way that dropped the service
            // files, which is common enough with shade and assembly to be worth naming.
            throw new AceMqException("no codecs were found on the classpath. This normally means a shaded or"
                    + " assembled jar dropped the META-INF/services entries; merge them rather than"
                    + " overwriting.");
        }
        // Sorted by the providers' own preference, then by name, so that the order a consumer
        // tries formats in does not depend on the order the classpath happened to be scanned.
        found.sort(Comparator.comparingInt(CodecProvider::order).thenComparing(CodecProvider::name));
        return found;
    }

    private static String available() {
        return String.join(", ", names());
    }
}
