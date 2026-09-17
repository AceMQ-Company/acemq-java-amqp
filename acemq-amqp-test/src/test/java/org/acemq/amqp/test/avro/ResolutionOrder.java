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
 * The reader side of the Avro resolution fixtures: what a consumer was compiled against.
 *
 * <p>Written by hand for the same reason as {@link TestOrder} — adding the Avro Maven plugin to
 * generate two classes would put code generation into every build — and it carries the one
 * property the fixtures are about: a schema of its own, which is the reader schema Avro resolves
 * a message onto.
 *
 * <p>{@code currency} defaults to {@code "GBP"} rather than to the empty string on purpose. A
 * default that is also the type's zero value passes whether resolution happened or not, because
 * an absent field and a field defaulted to {@code ""} look identical at the assertion. {@code
 * "GBP"} can only have come from this schema.
 */
public class ResolutionOrder extends SpecificRecordBase {

    /** The reader schema: two fields the writers all have, and one only this side declares. */
    public static final Schema SCHEMA$ = new Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"ResolutionOrder\",\"namespace\":\"org.acemq.amqp.test.avro\","
                    + "\"fields\":["
                    + "{\"name\":\"orderId\",\"type\":\"string\"},"
                    + "{\"name\":\"total\",\"type\":\"int\"},"
                    + "{\"name\":\"currency\",\"type\":\"string\",\"default\":\"GBP\"}]}");

    private CharSequence orderId;
    private int total;
    private CharSequence currency;

    /** Required: the codec builds one of these to read the schema off it. */
    public ResolutionOrder() {
        // deliberately empty
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0 :
                return orderId;
            case 1 :
                return total;
            case 2 :
                return currency;
            default :
                throw new org.apache.avro.AvroRuntimeException("no field with index " + field);
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0 :
                this.orderId = (CharSequence) value;
                break;
            case 1 :
                this.total = (Integer) value;
                break;
            case 2 :
                this.currency = (CharSequence) value;
                break;
            default :
                throw new org.apache.avro.AvroRuntimeException("no field with index " + field);
        }
    }

    public String orderId() {
        return orderId == null ? null : orderId.toString();
    }

    public int total() {
        return total;
    }

    public String currency() {
        return currency == null ? null : currency.toString();
    }
}
