/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
public class FlipStackTest {

    @Test
    public void testSimpleFlip() throws Exception {

        final FlipStack stack = new FlipStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        stack.flip();

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());
    }

    @Test
    public void testMultiFlip() throws Exception {

        final FlipStack stack = new FlipStack(1);

        for (int i = 0; i < 10; i++) {
            stack.push(i);
            stack.push(i + 1);
            stack.flip();
            assertEquals(i + 1, stack.pop());
            assertEquals(i, stack.pop());
        }
    }

    @Test
    public void testForEach() throws Exception {

        final FlipStack stack = new FlipStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        System.out.println(stack);
        stack.flip();
        System.out.println(stack);

        stack.forEach(stack::push);

        System.out.println(stack);

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());

        stack.flip();

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());

    }

    @Test
    public void testFillBoth() throws Exception {

        final FlipStack stack = new FlipStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        stack.flip();

        stack.push(4);
        stack.push(5);
        stack.push(6);

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());

        stack.flip();

        assertEquals(6, stack.pop());
        assertEquals(5, stack.pop());
        assertEquals(4, stack.pop());

    }
}
