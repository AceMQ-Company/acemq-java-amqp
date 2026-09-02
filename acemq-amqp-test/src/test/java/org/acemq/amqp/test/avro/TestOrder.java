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
package org.acemq.amqp.test.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * What the Avro compiler would emit, written by hand.
 *
 * <p>{@code AvroCodec.of(Class)} takes a generated class and had no test at all, because
 * nothing in this repository generated one. Adding the Avro Maven plugin for a single test
 * class would put code generation into every build for one path; this is the same contract
 * — a no-argument constructor, a schema of its own, and indexed get/put — in a file a
 * reader can follow.
 *
 * <p>The package and the class name have to match the schema's namespace and name. Avro
 * resolves the class from the schema when reading, so a mismatch produces a
 * {@code GenericRecord} where a specific one was asked for, which is a confusing way to fail.
 */
public class TestOrder extends SpecificRecordBase {

    public static final Schema SCHEMA$ = new Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"TestOrder\",\"namespace\":\"org.acemq.amqp.test.avro\","
                    + "\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"total\",\"type\":\"int\"}]}");

    private CharSequence id;
    private int total;

    /** Required: the codec builds one of these to read the schema off it. */
    public TestOrder() {
        // deliberately empty
    }

    public TestOrder(CharSequence id, int total) {
        this.id = id;
        this.total = total;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0 :
                return id;
            case 1 :
                return total;
            default :
                throw new org.apache.avro.AvroRuntimeException("no field with index " + field);
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0 :
                this.id = (CharSequence) value;
                break;
            case 1 :
                this.total = (Integer) value;
                break;
            default :
                throw new org.apache.avro.AvroRuntimeException("no field with index " + field);
        }
    }

    public String id() {
        return id == null ? null : id.toString();
    }

    public int total() {
        return total;
    }
}
