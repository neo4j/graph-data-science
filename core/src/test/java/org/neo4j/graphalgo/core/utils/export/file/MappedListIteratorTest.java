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
package org.neo4j.graphalgo.core.utils.export.file;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MappedListIteratorTest {

    @Test
    void shouldIterateThroughListMapping() {
        Map<String, List<Integer>> mapping = Map.of(
            "a", List.of(0, 1, 2),
            "b", List.of(3, 4, 5),
            "c", List.of(6)
        );
        var mappedListIterator = new MappedListIterator<>(mapping);
        Map<String, List<Integer>> actual = new HashMap<>();
        while(mappedListIterator.hasNext()) {
            Pair<String, Integer> entry = mappedListIterator.next();
            actual.computeIfAbsent(entry.getKey(), __ -> new ArrayList<>()).add(entry.getValue());
        }

        assertThat(actual).isEqualTo(mapping);
    }

}