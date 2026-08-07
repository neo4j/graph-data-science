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

import static org.assertj.core.api.Assertions.assertThat;

class FloatVectorImplTest {

    @Test
    void floatVectorValue() {
        var value = new FloatVectorImpl(new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        assertThat(value.floatVectorValue()).isEqualTo(new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
    }

    @Test
    void floatVectorValueIsACopy() {
        var value = new FloatVectorImpl(new float[]{1, 2, 3});

        var returned = value.floatVectorValue();
        returned[0] = 42;

        assertThat(value.floatVectorValue()).isEqualTo(new float[]{1, 2, 3});
    }

    @Test
    void floatValue() {
        var value = new FloatVectorImpl(new float[]{1, 2, 3});
        assertThat(value.floatValue(0)).isEqualTo(1);
        assertThat(value.floatValue(1)).isEqualTo(2);
        assertThat(value.floatValue(2)).isEqualTo(3);
    }

    @Test
    void dimension() {
        var value = new FloatVectorImpl(new float[]{1, 2, 3});
        assertThat(value.dimension()).isEqualTo(3);
    }

    @Test
    void type() {
        var value = new FloatVectorImpl(new float[]{1, 2, 3});
        assertThat(value.type()).isEqualTo(ValueType.FLOAT_VECTOR);
    }

    @Test
    void testEquals() {
        var value = new FloatVectorImpl(new float[]{1, 2, 3});
        assertThat(value).isEqualTo(new FloatVectorImpl(new float[]{1, 2, 3}));
        assertThat(value.equals(new float[]{1, 2, 3})).isTrue();
        assertThat(value.equals(new float[]{1, 2})).isFalse();
    }

    @Test
    void isEqualToTheFloatArrayHoldingTheSameCoordinates() {
        var vector = new FloatVectorImpl(new float[]{1, 2, 3});
        var array = new FloatArrayImpl(new float[]{1, 2, 3});

        assertThat(vector).isEqualTo(array);
        assertThat(array).isEqualTo(vector);
        assertThat(vector).hasSameHashCodeAs(array);
    }
}
