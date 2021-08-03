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
package org.neo4j.gds.core.utils.paged;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;

class HugeLongArrayQueueTest {

    @Test
    void testAdd() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.empty());
        q.add(42);
        q.add(1337);
        assertEquals(2, q.size());
    }

    @Test
    void testAddContinuously() {
        var capacity = 10;
        var q = HugeLongArrayQueue.newQueue(capacity, AllocationTracker.empty());
        for (int i = 0; i < capacity * 10; i++) {
            q.add(i);
            assertEquals(1, q.size());
            q.remove();
            assertEquals(0, q.size());
        }
    }

    @Test
    void testRemoveFromFullQueue() {
        var capacity = 10;
        var q = HugeLongArrayQueue.newQueue(capacity, AllocationTracker.empty());
        // fill up queue
        for (int i = 0; i < capacity; i++) {
            q.add(i);
        }
        assertEquals(capacity, q.size());
        q.remove();
        assertEquals(capacity - 1, q.size());
        q.add(42);
        assertEquals(capacity, q.size());
    }

    @Test
    void testRemove() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.empty());
        q.add(42);
        q.add(1337);
        assertEquals(42, q.remove());
        assertEquals(1337, q.remove());
        assertEquals(0, q.size());
    }

    @Test
    void testIsEmpty() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.empty());
        assertTrue(q.isEmpty());
        q.add(42);
        assertFalse(q.isEmpty());
        q.remove();
        assertTrue(q.isEmpty());
    }

    @Test
    void throwWhenEmpty() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.empty());
        var ex = assertThrows(IndexOutOfBoundsException.class, q::remove);
        assertEquals("Queue is empty.", rootCause(ex).getMessage());
    }

    @Test
    void throwWhenFull() {
        var q = HugeLongArrayQueue.newQueue(0, AllocationTracker.empty());
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> q.add(42));
        assertEquals("Queue is full.", rootCause(ex).getMessage());
    }
}
