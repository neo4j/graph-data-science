/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.api.schema;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.model.proto.GraphSchemaProto;

import static org.assertj.core.api.Assertions.assertThat;

class DoubleArrayDefaultValueSerializerTest {

    @Test
    void testSupportedType() {
        var serializer = new DoubleArrayDefaultValueSerializer();
        for (ValueType value : ValueType.values()) {
            if (value == ValueType.DOUBLE_ARRAY) {
                assertThat(serializer.canProcess(value)).isTrue();
            } else {
                assertThat(serializer.canProcess(value)).withFailMessage(value::name).isFalse();
            }
        }
    }

    @Test
    void testUnspecifiedDefaultValue() {
        var serializer = new DoubleArrayDefaultValueSerializer();
        var defaultValueBuilder = GraphSchemaProto.DefaultValue.newBuilder();
        serializer.processValue(DefaultValue.forDoubleArray(), defaultValueBuilder);
        var doubleArrayValue = defaultValueBuilder.getDoubleArrayValue();
        assertThat(doubleArrayValue.getHasValue()).isFalse();
        assertThat(doubleArrayValue.getDoubleArrayValueList()).isEmpty();
    }

    @Test
    void testSpecifiedDefaultValue() {
        var serializer = new DoubleArrayDefaultValueSerializer();
        var defaultValueBuilder = GraphSchemaProto.DefaultValue.newBuilder();
        serializer.processValue(DefaultValue.of(new double[] { 4.2D, 13.37D }), defaultValueBuilder);

        var doubleArrayValue = defaultValueBuilder.getDoubleArrayValue();
        assertThat(doubleArrayValue.getHasValue()).isTrue();
        assertThat(doubleArrayValue.getDoubleArrayValueList()).containsExactly(4.2D, 13.37D);
    }

}
