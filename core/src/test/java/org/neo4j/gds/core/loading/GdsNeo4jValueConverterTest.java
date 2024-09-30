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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.values.primitive.PrimitiveValues;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;

class GdsNeo4jValueConverterTest {

    @Test
    void shouldConvertFloatArray() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(new float[]{1, -1})))
            .isEqualTo(PrimitiveValues.floatArray(new float[]{1, -1}));
    }

    @Test
    void shouldConvertDoubleArray() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(new double[]{1, -1})))
            .isEqualTo(PrimitiveValues.doubleArray(new double[]{1, -1}));
    }
    @Test
    void shouldConvertLongArray() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(new long[]{1, -1})))
            .isEqualTo(PrimitiveValues.longArray(new long[]{1, -1}));
    }

    @Test
    void shouldConvertIntArray() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(new int[]{1, -1})))
            .isEqualTo(PrimitiveValues.intArray(new int[]{1, -1}));
    }

    @Test
    void shouldConvertShortArray() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(new short[]{1, -1})))
            .isEqualTo(PrimitiveValues.shortArray(new short[]{1, -1}));
    }

    @Test
    void shouldConvertByteArray() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(new byte[]{1, -1})))
            .isEqualTo(PrimitiveValues.byteArray(new byte[]{1, -1}));
    }

    @Test
    void shouldConvertFloat() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(1F)))
            .isEqualTo(PrimitiveValues.floatingPointValue(1F));
    }

    @Test
    void shouldConvertDouble() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(1D)))
            .isEqualTo(PrimitiveValues.floatingPointValue(1D));
    }

    @Test
    void shouldConvertInt() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(1)))
            .isEqualTo(PrimitiveValues.longValue(1));
    }

    @Test
    void shouldConvertLong() {
        assertThat(GdsNeo4jValueConverter.toValue(Values.of(1L)))
            .isEqualTo(PrimitiveValues.longValue(1L));
    }

}
