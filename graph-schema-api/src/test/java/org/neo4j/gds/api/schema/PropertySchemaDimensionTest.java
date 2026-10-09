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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PropertySchemaDimensionTest {

    @Test
    void defaultsToNoDimension() {
        assertThat(NodePropertySchema.of("scores", ValueType.FLOAT_ARRAY).dimension()).isEmpty();
    }

    @Test
    void rejectsVectorWithoutDimension() {
        assertThatThrownBy(() -> NodePropertySchema.of("embedding", ValueType.FLOAT_VECTOR).dimension())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must have a dimension, but has none.");
    }

    @Test
    void carriesADimensionForAVectorType() {
        var schema = vector(ValueType.FLOAT_VECTOR, 128);

        assertThat(schema.dimension()).hasValue(128);
    }

    @Test
    void rejectsADimensionOnANonVectorType() {
        assertThatThrownBy(() -> vector(ValueType.FLOAT_ARRAY, 128))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot have a dimension");
    }

    @Test
    void rejectsANonPositiveDimension() {
        assertThatThrownBy(() -> vector(ValueType.FLOAT_VECTOR, 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("positive dimension");
    }

    @ParameterizedTest
    @EnumSource(value = ValueType.class, names = {"FLOAT_VECTOR", "DOUBLE_VECTOR"})
    void acceptsADefaultValueMatchingTheDimension(ValueType valueType) {
        var schema = vectorWithDefault(valueType, DefaultValue.of(new double[]{1.0, 2.0, 3.0}), 3);

        assertThat(schema.defaultValue().doubleArrayValue()).containsExactly(1.0, 2.0, 3.0);
    }

    @ParameterizedTest
    @EnumSource(value = ValueType.class, names = {"FLOAT_VECTOR", "DOUBLE_VECTOR"})
    void rejectsADefaultValueNotMatchingTheDimension(ValueType valueType) {
        assertThatThrownBy(() -> vectorWithDefault(valueType, DefaultValue.of(new float[]{1.0F, 2.0F}), 3))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("has dimension 3, but its default value has 2 elements.");
    }

    @Test
    void dimensionParticipatesInEquality() {
        assertThat(vector(ValueType.FLOAT_VECTOR, 128)).isNotEqualTo(vector(ValueType.FLOAT_VECTOR, 64));
        assertThat(vector(ValueType.FLOAT_VECTOR, 128)).isEqualTo(vector(ValueType.FLOAT_VECTOR, 128));
    }

    private static NodePropertySchema vector(ValueType valueType, int dimension) {
        return vectorWithDefault(valueType, valueType.fallbackValue(), dimension);
    }

    private static NodePropertySchema vectorWithDefault(ValueType valueType, DefaultValue defaultValue, int dimension) {
        return NodePropertySchema.of(
            "embedding",
            valueType,
            defaultValue,
            PropertyState.PERSISTENT,
            OptionalInt.of(dimension)
        );
    }
}
