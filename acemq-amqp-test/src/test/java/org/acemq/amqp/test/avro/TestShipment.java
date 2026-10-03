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
 * A generated-style record holding another record and an enum, written by hand like
 * {@link TestOrder}. Avro resolves each of those by name when reading, so this is what proves
 * the codec resolves nested types of a caller's class and not only the top-level one.
 */
public class TestShipment extends SpecificRecordBase {

    public static final Schema SCHEMA$ = new Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"TestShipment\",\"namespace\":\"org.acemq.amqp.test.avro\","
                    + "\"fields\":["
                    + "{\"name\":\"order\",\"type\":" + TestOrder.SCHEMA$ + "},"
                    + "{\"name\":\"priority\",\"type\":{\"type\":\"enum\",\"name\":\"TestPriority\","
                    + "\"symbols\":[\"LOW\",\"HIGH\"]}}]}");

    private TestOrder order;
    private TestPriority priority;

    /** Required: the codec builds one of these to read the schema off it. */
    public TestShipment() {
        // deliberately empty
    }

    public TestShipment(TestOrder order, TestPriority priority) {
        this.order = order;
        this.priority = priority;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0 :
                return order;
            case 1 :
                return priority;
            default :
                throw new org.apache.avro.AvroRuntimeException("no field with index " + field);
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0 :
                this.order = (TestOrder) value;
                break;
            case 1 :
                this.priority = (TestPriority) value;
                break;
            default :
                throw new org.apache.avro.AvroRuntimeException("no field with index " + field);
        }
    }

    public TestOrder order() {
        return order;
    }

    public TestPriority priority() {
        return priority;
    }
}
