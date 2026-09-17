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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * The smallest JSON writer that produces stable bytes.
 *
 * <p>Written here rather than pulled in, because the fixtures are compared byte for byte and a
 * library that reorders keys, changes how it renders a double or decides to pretty-print
 * differently between versions would turn a dependency bump into a five-repository change.
 *
 * <p>It began as a private class inside {@link ContractFixtures} and moved out when a second
 * generator needed it. Two copies of the renderer would mean two fixtures that look alike until
 * one of them is reformatted, which is the same failure the fixtures exist to prevent, one level
 * down. It is public only because the generator that shares it lives in
 * {@code org.acemq.amqp.test}; nothing outside the test tree can see this class at all.
 */
public final class Json {

    private Json() {
        throw new AssertionError("Json is a helper and must not be instantiated");
    }

    /**
     * @param value a map, a list or a scalar
     * @return the value rendered as JSON, indented two spaces and ending in a newline
     */
    public static String render(Object value) {
        StringBuilder out = new StringBuilder();
        write(out, value, 0);
        out.append('\n');
        return out.toString();
    }

    private static void write(StringBuilder out, Object value, int depth) {
        if (value instanceof Map) {
            writeObject(out, (Map<?, ?>) value, depth);
        } else if (value instanceof List) {
            writeArray(out, (List<?>) value, depth);
        } else {
            writeScalar(out, value);
        }
    }

    private static void writeObject(StringBuilder out, Map<?, ?> map, int depth) {
        if (map.isEmpty()) {
            out.append("{}");
            return;
        }
        out.append("{\n");
        int i = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            indent(out, depth + 1);
            writeScalar(out, String.valueOf(entry.getKey()));
            out.append(": ");
            write(out, entry.getValue(), depth + 1);
            out.append(++i < map.size() ? ",\n" : "\n");
        }
        indent(out, depth);
        out.append('}');
    }

    private static void writeArray(StringBuilder out, List<?> list, int depth) {
        if (list.isEmpty()) {
            out.append("[]");
            return;
        }
        // Scalars stay on one line. A schedule is read as a sequence and reads as one.
        boolean scalars = true;
        for (Object element : list) {
            scalars &= !(element instanceof Map) && !(element instanceof List);
        }
        if (scalars) {
            out.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    out.append(", ");
                }
                writeScalar(out, list.get(i));
            }
            out.append(']');
            return;
        }
        out.append("[\n");
        for (int i = 0; i < list.size(); i++) {
            indent(out, depth + 1);
            write(out, list.get(i), depth + 1);
            out.append(i < list.size() - 1 ? ",\n" : "\n");
        }
        indent(out, depth);
        out.append(']');
    }

    private static void writeScalar(StringBuilder out, Object value) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof Boolean || value instanceof Long || value instanceof Integer) {
            out.append(value);
        } else if (value instanceof Double || value instanceof Float) {
            // Shortest exact rendering, so 0.2 is "0.2" rather than "0.2000000000000000111".
            out.append(new BigDecimal(value.toString()).stripTrailingZeros().toPlainString());
        } else {
            out.append('"')
                    .append(String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\""))
                    .append('"');
        }
    }

    private static void indent(StringBuilder out, int depth) {
        for (int i = 0; i < depth; i++) {
            out.append("  ");
        }
    }
}
