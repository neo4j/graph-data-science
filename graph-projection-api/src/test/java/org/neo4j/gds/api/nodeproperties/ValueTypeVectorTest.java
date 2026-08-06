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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

class ValueTypeVectorTest {

    private static final EnumSet<ValueType> VECTOR_TYPES =
        EnumSet.of(ValueType.FLOAT_VECTOR, ValueType.DOUBLE_VECTOR);

    @ParameterizedTest
    @EnumSource(ValueType.class)
    void onlyVectorTypesAreVectors(ValueType valueType) {
        assertThat(valueType.isVector()).isEqualTo(VECTOR_TYPES.contains(valueType));
    }

    @ParameterizedTest
    @CsvSource({
        "FLOAT_VECTOR, float_vector",
        "DOUBLE_VECTOR, double_vector"
    })
    void vectorTypesHaveTheirOwnCsvName(ValueType vector, String csvName) {
        assertThat(vector.csvName()).isEqualTo(csvName);
        assertThat(ValueType.fromCsvName(csvName)).isSameAs(vector);
    }

    @Test
    void arrayCsvNamesStillResolveToTheArrayTypes() {
        assertThat(ValueType.fromCsvName("float[]")).isSameAs(ValueType.FLOAT_ARRAY);
        assertThat(ValueType.fromCsvName("double[]")).isSameAs(ValueType.DOUBLE_ARRAY);
    }

    @Test
    void vectorIsNotCompatibleWithAMismatchedArray() {
        assertThat(ValueType.FLOAT_VECTOR.isCompatibleWith(ValueType.DOUBLE_ARRAY)).isFalse();
        assertThat(ValueType.DOUBLE_VECTOR.isCompatibleWith(ValueType.FLOAT_ARRAY)).isFalse();
        assertThat(ValueType.FLOAT_VECTOR.isCompatibleWith(ValueType.DOUBLE_VECTOR)).isFalse();
        assertThat(ValueType.FLOAT_VECTOR.isCompatibleWith(ValueType.LONG_ARRAY)).isFalse();
    }

    @Test
    void compatibilityOfNonVectorTypesIsUnchanged() {
        for (ValueType left : ValueType.values()) {
            for (ValueType right : ValueType.values()) {
                if (left.isVector() || right.isVector() || left == ValueType.UNTYPED_ARRAY) {
                    continue;
                }
                assertThat(left.isCompatibleWith(right))
                    .as("%s isCompatibleWith %s", left, right)
                    .isEqualTo(left == right);
            }
        }
    }
    
    @Test
    void aVisitorCanDistinguishVectors() {
        var visitor = new ValueType.Visitor<String>() {
            @Override
            public String visitLong() {
                return "long";
            }

            @Override
            public String visitDouble() {
                return "double";
            }

            @Override
            public String visitString() {
                return "string";
            }

            @Override
            public String visitLongArray() {
                return "long[]";
            }

            @Override
            public String visitDoubleArray() {
                return "double[]";
            }

            @Override
            public String visitFloatArray() {
                return "float[]";
            }

            @Override
            public String visitFloatVector() {
                return "float_vector";
            }

            @Override
            public String visitDoubleVector() {
                return "double_vector";
            }
        };

        assertThat(ValueType.FLOAT_VECTOR.accept(visitor)).isEqualTo("float_vector");
        assertThat(ValueType.DOUBLE_VECTOR.accept(visitor)).isEqualTo("double_vector");
        assertThat(ValueType.FLOAT_ARRAY.accept(visitor)).isEqualTo("float[]");
    }
}
