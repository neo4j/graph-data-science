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
package org.neo4j.gds.similarity.knn;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NeighborListTest {

    @Test
    void shouldKeepMaxValues() {
        long[] expected = {6L, 5L, 2L};

        NeighborList queue = new NeighborList(3);
        SplittableRandom splittableRandom = new SplittableRandom();

        assertEquals(1, queue.add(0, Double.MIN_VALUE, splittableRandom));
        assertEquals(1, queue.add(6, 6.0, splittableRandom));
        assertEquals(1, queue.add(1, 1.0, splittableRandom));
        assertEquals(1, queue.add(5, 5.0, splittableRandom));
        assertEquals(1, queue.add(2, 4.0, splittableRandom));
        assertEquals(0, queue.add(4, 2.0, splittableRandom));
        assertEquals(0, queue.add(3, 3.0, splittableRandom));

        long[] actual = queue.elements().toArray();
        assertArrayEquals(expected, actual);
    }

    @Test
    void shouldLimitReturnWhenNotFull() {
        long[] expected = {6L, 5L, 4L};

        NeighborList queue = new NeighborList(10);
        SplittableRandom splittableRandom = new SplittableRandom();

        assertEquals(1, queue.add(6, 6.0, splittableRandom));
        assertEquals(1, queue.add(5, 5.0, splittableRandom));
        assertEquals(1, queue.add(4, 4.0, splittableRandom));

        long[] actual = queue.elements().toArray();
        assertArrayEquals(expected, actual);
    }

    @Test
    void insertEverything() {
        var nodeCount = 42;
        var elements = LongStream.range(0, nodeCount).boxed().collect(Collectors.toList());
        var rng = new SplittableRandom(1337L);

        var queue = new NeighborList(nodeCount);

        elements.forEach(candidate -> queue.add(candidate, 1.0 / (1.0 + Math.abs(candidate - 2)), rng));

        assertThat(queue.elements()).containsExactlyInAnyOrderElementsOf(elements);
    }

    @Test
    void insertEveryThingTake2() {
        List<Long> elements = List.of(0L, 2L);
        var queue = new NeighborList(2);
        var rng = new SplittableRandom(1337L);

        elements.forEach(candidate -> queue.add(candidate, 1.0 / (1.0 + Math.abs(candidate - 1)), rng));

        assertThat(queue.elements()).containsExactlyInAnyOrderElementsOf(elements);
    }

}
