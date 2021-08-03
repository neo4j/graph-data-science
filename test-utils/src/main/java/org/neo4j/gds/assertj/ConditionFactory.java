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
package org.neo4j.gds.assertj;

import org.assertj.core.api.Condition;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public final class ConditionFactory {

    private ConditionFactory() {}

    public static <K, V> Condition<Map<K, V>> containsAllEntriesOf(Map<K, V> map) {
        return new Condition<>(m -> {
            assertThat(m).containsAllEntriesOf(map);
            return true;
        }, "a map containing all entries of %s", map.toString());
    }

    public static <K, V> Condition<Map<K, V>> containsExactlyInAnyOrderEntriesOf(Map<K, V> map) {
        return new Condition<>(m -> {
            assertThat(m).containsExactlyInAnyOrderEntriesOf(map);
            return true;
        }, "a map containing all entries of %s", map.toString());
    }

    public static Condition<double[]> hasSize(int size) {
        return new Condition<>(array -> {
            assertThat(array).hasSize(size);
            return true;
        }, "a double array of size %s", size);
    }
}
