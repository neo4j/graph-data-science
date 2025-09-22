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
package org.neo4j.gds.maxflow;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AtomicWorkingSetTest {
    @Test
    void shouldPushAndPullElements() {
        var workingSet = new AtomicWorkingSet(5);

        workingSet.push(42);
        workingSet.push(43);

        assertEquals(42, workingSet.pop());
        assertEquals(43, workingSet.pop());
        assertEquals(-1L, workingSet.pop()); // Should return -1 when no more elements
    }

    @Test
    void shouldBatchPushElements() {
        var workingSet = new AtomicWorkingSet(5);
        var queue = HugeLongArrayQueue.newQueue(5);
        queue.add(1);
        queue.add(2);
        queue.add(3);

        workingSet.batchPush(queue);

        assertEquals(1, workingSet.pop());
        assertEquals(2, workingSet.pop());
        assertEquals(3, workingSet.pop());
        assertEquals(-1L, workingSet.pop());
    }

    @Test
    void shouldReportEmptyCorrectly() {
        var workingSet = new AtomicWorkingSet(5);

        assertTrue(workingSet.isEmpty());

        workingSet.push(1);
        assertFalse(workingSet.isEmpty());

        workingSet.pop();
        assertTrue(workingSet.isEmpty());
    }

    @Test
    void shouldHandleCapacityLimit() {
        var workingSet = new AtomicWorkingSet(2);

        workingSet.push(1);
        workingSet.push(2);

        assertEquals(1, workingSet.pop());
        assertEquals(2, workingSet.pop());
        assertEquals(-1L, workingSet.pop());
    }
}
