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
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.model.proto.GraphSchemaProto;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaDeserializerTest {

    @Test
    void shouldDeserializeNullAsDefaultDoubleArrayValue() {
        var serializer = new DoubleArrayDefaultValueSerializer();
        var defaultValueBuilder = GraphSchemaProto.DefaultValue.newBuilder();
        serializer.processValue(DefaultValue.forDoubleArray(), defaultValueBuilder);

        var deserializedValue = SchemaDeserializer.serializedDefaultValue(ValueType.DOUBLE_ARRAY, defaultValueBuilder.build());

        assertThat(deserializedValue).isEqualTo(DefaultValue.forDoubleArray().doubleArrayValue());
    }

    @Test
    void shouldDeserializeNullAsDefaultFloatArrayValue() {
        var serializer = new FloatArrayDefaultValueSerializer();
        var defaultValueBuilder = GraphSchemaProto.DefaultValue.newBuilder();
        serializer.processValue(DefaultValue.forFloatArray(), defaultValueBuilder);

        var deserializedValue = SchemaDeserializer.serializedDefaultValue(ValueType.FLOAT_ARRAY, defaultValueBuilder.build());

        assertThat(deserializedValue).isEqualTo(DefaultValue.forFloatArray().floatArrayValue());
    }

    @Test
    void shouldDeserializeNullAsDefaultLongArrayValue() {
        var serializer = new LongArrayDefaultValueSerializer();
        var defaultValueBuilder = GraphSchemaProto.DefaultValue.newBuilder();
        serializer.processValue(DefaultValue.forLongArray(), defaultValueBuilder);

        var deserializedValue = SchemaDeserializer.serializedDefaultValue(ValueType.LONG_ARRAY, defaultValueBuilder.build());

        assertThat(deserializedValue).isEqualTo(DefaultValue.forLongArray().longArrayValue());
    }
}
