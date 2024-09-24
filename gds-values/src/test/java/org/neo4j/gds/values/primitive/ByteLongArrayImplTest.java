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

class ByteLongArrayImplTest {

    @Test
    void longArrayValue() {
        var value = new ByteLongArrayImpl(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        assertThat(value.longArrayValue()).isEqualTo(new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
    }

    @Test
    void longValue() {
        var value = new ByteLongArrayImpl(new byte[]{1, 2, 3});
        assertThat(value.longValue(0)).isEqualTo(1);
        assertThat(value.longValue(1)).isEqualTo(2);
        assertThat(value.longValue(2)).isEqualTo(3);
    }

    @Test
    void length() {
        var value = new ByteLongArrayImpl(new byte[]{1, 2, 3});
        assertThat(value.length()).isEqualTo(3);
    }

    @Test
    void type() {
        var value = new ByteLongArrayImpl(new byte[]{1, 2, 3});
        assertThat(value.type()).isEqualTo(ValueType.LONG_ARRAY);
    }

    @Test
    void testEquals() {
        // empty is equal to the empty constant
        assertThat(new ByteLongArrayImpl(new byte[]{})).isEqualTo(PrimitiveValues.EMPTY_LONG_ARRAY);

        var value = new ByteLongArrayImpl(new byte[]{1, 2, 3});
        // equal to another instance if the held values are equal
        assertThat(value).isEqualTo(new ByteLongArrayImpl(new byte[]{1, 2, 3}));
        // equal to an array if the held array is equal
        assertThat(value.equals(new byte[]{1, 2, 3})).isTrue();
    }
}
