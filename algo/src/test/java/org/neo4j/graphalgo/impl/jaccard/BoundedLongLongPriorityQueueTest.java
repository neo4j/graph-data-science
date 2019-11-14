/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.jaccard;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoundedLongLongPriorityQueueTest {

    @Test
    void shouldKeepMinValues() {
        List<Long> expected = new ArrayList<>();
        expected.add(0L);
        expected.add(1L);
        expected.add(2L);

        BoundedLongLongPriorityQueue queue = BoundedLongLongPriorityQueue.min(3);

        queue.offer(0, 0, 0.0);
        queue.offer(6, 6, 6.0);
        queue.offer(1, 1, 1.0);
        queue.offer(5, 5, 5.0);
        queue.offer(2, 2, 2.0);
        queue.offer(4, 4, 4.0);
        queue.offer(3, 3, 3.0);

        assertResults(expected, queue);
    }

    @Test
    void shouldKeepMaxValues() {
        List<Long> expected = new ArrayList<>();
        expected.add(6L);
        expected.add(5L);
        expected.add(4L);

        BoundedLongLongPriorityQueue queue = BoundedLongLongPriorityQueue.max(3);

        queue.offer(0, 0, 0.0);
        queue.offer(6, 6, 6.0);
        queue.offer(1, 1, 1.0);
        queue.offer(5, 5, 5.0);
        queue.offer(2, 2, 2.0);
        queue.offer(4, 4, 4.0);
        queue.offer(3, 3, 3.0);

        assertResults(expected, queue);
    }

    @Test
    void shouldLimitReturnWhenNotFull() {
        List<Long> expected = new ArrayList<>();
        expected.add(6L);
        expected.add(5L);
        expected.add(4L);

        BoundedLongLongPriorityQueue queue = BoundedLongLongPriorityQueue.max(10);

        queue.offer(6, 6, 6.0);
        queue.offer(5, 5, 5.0);
        queue.offer(4, 4, 4.0);

        assertResults(expected, queue);
    }

    private void assertResults(List<Long> expected, BoundedLongLongPriorityQueue queue) {
        List<Double> expectedPriorities = expected.stream().map(Long::doubleValue).collect(Collectors.toList());

        List<Long> actualElements1 = queue.elements1().boxed().collect(Collectors.toList());
        List<Long> actualElements2 = queue.elements2().boxed().collect(Collectors.toList());
        List<Double> actualPriorities = queue.priorities().boxed().collect(Collectors.toList());

        assertEquals(expected, actualElements1);
        assertEquals(expected, actualElements2);
        assertEquals(expectedPriorities, actualPriorities);
    }
}
