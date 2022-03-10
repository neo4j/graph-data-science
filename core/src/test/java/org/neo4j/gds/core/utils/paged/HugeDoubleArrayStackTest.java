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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;

class HugeDoubleArrayStackTest {

    @Test
    void testPush() {
        var q = HugeDoubleArrayStack.newStack(10);
        q.push(42.2);
        q.push(13.37);
        assertEquals(2, q.size());
    }

    @Test
    void testPop() {
        var q = HugeDoubleArrayStack.newStack(10);
        q.push(4.2);
        q.push(13.37);
        assertEquals(13.37, q.pop());
        assertEquals(4.2, q.pop());
        assertEquals(0, q.size());
    }

    @Test
    void testPopFromFullStack() {
        var capacity = 10;
        var q = HugeDoubleArrayStack.newStack(capacity);
        // fill up stack
        for (int i = 0; i < capacity; i++) {
            q.push(i);
        }
        assertEquals(capacity, q.size());
        q.pop();
        assertEquals(capacity - 1, q.size());
        q.push(42.7);
        assertEquals(capacity, q.size());
    }

    @Test
    void testIsEmpty() {
        var q = HugeDoubleArrayStack.newStack(10);
        assertTrue(q.isEmpty());
        q.push(4.2);
        assertFalse(q.isEmpty());
        q.pop();
        assertTrue(q.isEmpty());
    }

    @Test
    void throwWhenEmpty() {
        var q = HugeDoubleArrayStack.newStack(10);
        var ex = assertThrows(IndexOutOfBoundsException.class, q::pop);
        assertEquals("Stack is empty.", rootCause(ex).getMessage());
    }

    @Test
    void throwWhenFull() {
        var q = HugeDoubleArrayStack.newStack(0);
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> q.push(42));
        assertEquals("Stack is full.", rootCause(ex).getMessage());
    }
}
