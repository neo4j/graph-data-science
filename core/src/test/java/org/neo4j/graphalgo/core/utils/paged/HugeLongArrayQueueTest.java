/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

class HugeLongArrayQueueTest {

    @Test
    void testAdd() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.EMPTY);
        q.add(42);
        q.add(1337);
        assertEquals(2, q.size());
    }

    @Test
    void testRemove() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.EMPTY);
        q.add(42);
        q.add(1337);
        assertEquals(42, q.remove());
        assertEquals(1337, q.remove());
        assertEquals(0, q.size());
    }

    @Test
    void throwWhenEmpty() {
        var q = HugeLongArrayQueue.newQueue(10, AllocationTracker.EMPTY);
        var ex = assertThrows(IndexOutOfBoundsException.class, q::remove);
        assertEquals("Queue is empty.", rootCause(ex).getMessage());
    }

    @Test
    void throwWhenFull() {
        var q = HugeLongArrayQueue.newQueue(0, AllocationTracker.EMPTY);
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> q.add(42));
        assertEquals("Queue is full.", rootCause(ex).getMessage());
    }
}