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

import java.util.SplittableRandom;

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

}
