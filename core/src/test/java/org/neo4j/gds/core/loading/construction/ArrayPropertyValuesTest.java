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
package org.neo4j.gds.core.loading.construction;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.primitive.PrimitiveValues;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrayPropertyValuesTest {

    @Test
    void shouldExposePresentProperties() {
        var keys = new String[]{"foo", "bar", "baz"};
        var values = new GdsValue[]{
            PrimitiveValues.longValue(42L),
            null,
            PrimitiveValues.floatingPointValue(13.37D)
        };

        var propertyValues = new ArrayPropertyValues(keys, values);

        assertThat(propertyValues.isEmpty()).isFalse();
        assertThat(propertyValues.size()).isEqualTo(3);
        assertThat(propertyValues.propertyKeys()).containsExactly("foo", "baz");
        assertThat(propertyValues.get("foo")).isEqualTo(PrimitiveValues.longValue(42L));
        assertThat(propertyValues.get("bar")).isNull();
        assertThat(propertyValues.get("baz")).isEqualTo(PrimitiveValues.floatingPointValue(13.37D));
        assertThat(propertyValues.get("unknown")).isNull();

        Map<String, GdsValue> consumed = new HashMap<>();
        propertyValues.forEach(consumed::put);
        assertThat(consumed).containsOnlyKeys("foo", "baz");
    }

    @Test
    void shouldRejectMismatchedArrayLengths() {
        assertThatThrownBy(() -> new ArrayPropertyValues(
            new String[]{"foo", "bar"},
            new GdsValue[]{PrimitiveValues.longValue(42L)}
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldReturnSingleValue() {
        var propertyValues = new ArrayPropertyValues(
            new String[]{"bar"},
            new GdsValue[]{PrimitiveValues.longValue(42L)}
        );

        assertThat(propertyValues.size()).isEqualTo(1);
        assertThat(propertyValues.getSingle()).isEqualTo(PrimitiveValues.longValue(42L));
    }
}
