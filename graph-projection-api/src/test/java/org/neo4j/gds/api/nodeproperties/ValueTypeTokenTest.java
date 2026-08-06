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
package org.neo4j.gds.api.nodeproperties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueTypeTokenTest {

    @Test
    void formatsAVectorWithItsDimension() {
        assertThat(ValueTypeToken.format(ValueType.FLOAT_VECTOR, OptionalInt.of(128)))
            .isEqualTo("float_vector(128)");
        assertThat(ValueTypeToken.format(ValueType.DOUBLE_VECTOR, OptionalInt.of(3)))
            .isEqualTo("double_vector(3)");
    }

    @Test
    void formatsAVectorWithoutADimensionAsTheBareName() {
        assertThat(ValueTypeToken.format(ValueType.FLOAT_VECTOR, OptionalInt.empty()))
            .isEqualTo("float_vector");
        assertThat(ValueTypeToken.format(ValueType.FLOAT_VECTOR)).isEqualTo("float_vector");
    }

    @ParameterizedTest
    @EnumSource(value = ValueType.class, names = {"LONG", "DOUBLE", "STRING", "FLOAT_ARRAY", "DOUBLE_ARRAY", "LONG_ARRAY"})
    void formatsNonVectorTypesAsTheirCsvName(ValueType valueType) {
        assertThat(ValueTypeToken.format(valueType)).isEqualTo(valueType.csvName());
        // a dimension is ignored for a type that cannot carry one
        assertThat(ValueTypeToken.format(valueType, OptionalInt.of(7))).isEqualTo(valueType.csvName());
    }

    @Test
    void parsesAVectorWithItsDimension() {
        assertThat(ValueTypeToken.parse("float_vector(128)"))
            .isEqualTo(ValueTypeToken.of(ValueType.FLOAT_VECTOR, 128));
        assertThat(ValueTypeToken.parse("double_vector(3)"))
            .isEqualTo(ValueTypeToken.of(ValueType.DOUBLE_VECTOR, 3));
    }

    @Test
    void parsesABareVectorName() {
        assertThat(ValueTypeToken.parse("float_vector"))
            .isEqualTo(ValueTypeToken.of(ValueType.FLOAT_VECTOR));
        assertThat(ValueTypeToken.parse("float_vector").dimension()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = ValueType.class, names = {"LONG", "DOUBLE", "STRING", "FLOAT_ARRAY", "DOUBLE_ARRAY", "LONG_ARRAY"})
    void roundTripsNonVectorTypes(ValueType valueType) {
        assertThat(ValueTypeToken.parse(ValueTypeToken.format(valueType)))
            .isEqualTo(ValueTypeToken.of(valueType));
    }

    @Test
    void roundTripsAVector() {
        var token = ValueTypeToken.of(ValueType.FLOAT_VECTOR, 42);
        assertThat(ValueTypeToken.parse(ValueTypeToken.format(token.valueType(), token.dimension())))
            .isEqualTo(token);
    }

    @Test
    void rejectsADimensionOnANonVectorType() {
        assertThatThrownBy(() -> ValueTypeToken.parse("float[](3)"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not take a dimension");
    }

    @Test
    void rejectsAMissingClosingParenthesis() {
        assertThatThrownBy(() -> ValueTypeToken.parse("float_vector(3"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("missing the closing");
    }

    @Test
    void rejectsANonNumericDimension() {
        assertThatThrownBy(() -> ValueTypeToken.parse("float_vector(many)"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not a number");
    }

    @ParameterizedTest
    @ValueSource(strings = {"float_vector(0)", "float_vector(-1)"})
    void rejectsANonPositiveDimension(String token) {
        assertThatThrownBy(() -> ValueTypeToken.parse(token))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("positive dimension");
    }
}
