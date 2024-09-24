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
package org.neo4j.gds.values.primitive;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.values.FloatingPointValue;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrimitiveValuesTest {

    @Test
    void shouldCreateLongValuesFromScalarIntegers() {
        assertThat(PrimitiveValues.longValue(42))
            .satisfies(value -> {
                assertThat(value.type()).isEqualTo(ValueType.LONG);
                assertThat(value).isEqualTo(PrimitiveValues.create((byte) 42));
                assertThat(value).isEqualTo(PrimitiveValues.create((short) 42));
                assertThat(value).isEqualTo(PrimitiveValues.create((int) 42));
                assertThat(value).isEqualTo(PrimitiveValues.create((long) 42));
            });
    }

    @Test
    void shouldCreateDoubleValuesFromScalarFloatingPoints() {
        FloatingPointValue floatValue = PrimitiveValues.floatingPointValue(1F);
        assertThat(floatValue).satisfies(value -> {
            assertThat(value.type()).isEqualTo(ValueType.DOUBLE);
            assertThat(value.doubleValue()).isEqualTo(1F);
            assertThat(value).isEqualTo(PrimitiveValues.create(1F));
        });
        FloatingPointValue doubleValue = PrimitiveValues.floatingPointValue(1D);
        assertThat(doubleValue).satisfies(value -> {
            assertThat(value.type()).isEqualTo(ValueType.DOUBLE);
            assertThat(value.doubleValue()).isEqualTo(1D);
            assertThat(value).isEqualTo(PrimitiveValues.create(1D));
        });
        assertThat(floatValue).isEqualTo(doubleValue);
    }

    @Test
    void shouldFailCreatingValuesFromSomeOtherThings() {
        assertThatThrownBy(() -> PrimitiveValues.create('c'))
            .hasMessageContaining("java.lang.Character")
            .hasMessageContaining("is not a supported property value");
        assertThatThrownBy(() -> PrimitiveValues.create("string"))
            .hasMessageContaining("java.lang.String")
            .hasMessageContaining("is not a supported property value");
        assertThatThrownBy(() -> PrimitiveValues.create(List.of(1, 2)))
            .hasMessageContaining("[1, 2]")
            .hasMessageContaining("List")
            .hasMessageContaining("is not a supported property value");
        assertThatThrownBy(() -> PrimitiveValues.create(new Object[]{'c', "string"}))
            .hasMessageContaining("[Ljava.lang.Object;")
            .hasMessageContaining("is not a supported property value");
    }

    @Test
    void shouldCreateValuesFromIntegerArrays() {
        var byteArray = PrimitiveValues.byteArray(new byte[]{1, 2, 3});
        assertThat(byteArray)
            .satisfies(arr -> {
                assertThat(arr.type()).isEqualTo(ValueType.LONG_ARRAY);
                assertThat(arr).isEqualTo(PrimitiveValues.create(new byte[]{1, 2, 3}));
                assertThat(arr).isEqualTo(PrimitiveValues.create(new Byte[]{(byte) 1, (byte) 2, (byte) 3}));
            });
        var shortArray = PrimitiveValues.shortArray(new short[]{1, 2, 3});
        assertThat(shortArray)
            .satisfies(arr -> {
                assertThat(arr.type()).isEqualTo(ValueType.LONG_ARRAY);
                assertThat(arr).isEqualTo(PrimitiveValues.create(new short[]{1, 2, 3}));
                assertThat(arr).isEqualTo(PrimitiveValues.create(new Short[]{(short) 1, (short) 2, (short) 3}));
            });
        var intArray = PrimitiveValues.intArray(new int[]{1, 2, 3});
        assertThat(intArray)
            .satisfies(arr -> {
                assertThat(arr.type()).isEqualTo(ValueType.LONG_ARRAY);
                assertThat(arr).isEqualTo(PrimitiveValues.create(new int[]{1, 2, 3}));
                assertThat(arr).isEqualTo(PrimitiveValues.create(new Integer[]{(int) 1, (int) 2, (int) 3}));
            });
        var longArray = PrimitiveValues.longArray(new long[]{1, 2, 3});
        assertThat(longArray)
            .satisfies(arr -> {
                assertThat(arr.type()).isEqualTo(ValueType.LONG_ARRAY);
                assertThat(arr).isEqualTo(PrimitiveValues.create(new long[]{1, 2, 3}));
                assertThat(arr).isEqualTo(PrimitiveValues.create(new Long[]{(long) 1, (long) 2, (long) 3}));
            });
        assertThat(longArray)
            .isEqualTo(byteArray)
            .isEqualTo(shortArray)
            .isEqualTo(intArray);
    }

    @Test
    void shouldCreateValuesFromFloatingPointArrays() {
        var floatArray = PrimitiveValues.floatArray(new float[]{1, 2, 3});
        assertThat(floatArray)
            .satisfies(arr -> {
                assertThat(arr.type()).isEqualTo(ValueType.FLOAT_ARRAY);
                assertThat(arr).isEqualTo(PrimitiveValues.create(new float[]{1, 2, 3}));
                assertThat(arr).isEqualTo(PrimitiveValues.create(new Float[]{(float) 1, (float) 2, (float) 3}));
            });
        var doubleArray = PrimitiveValues.doubleArray(new double[]{1, 2, 3});
        assertThat(doubleArray)
            .satisfies(arr -> {
                assertThat(arr.type()).isEqualTo(ValueType.DOUBLE_ARRAY);
                assertThat(arr).isEqualTo(PrimitiveValues.create(new double[]{1, 2, 3}));
                assertThat(arr).isEqualTo(PrimitiveValues.create(new Double[]{(double) 1, (double) 2, (double) 3}));
            });
        assertThat(doubleArray)
            .isEqualTo(floatArray);
    }
}
