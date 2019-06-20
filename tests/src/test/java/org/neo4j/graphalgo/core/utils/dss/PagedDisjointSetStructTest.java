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
package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
public class PagedDisjointSetStructTest {

    private PagedDisjointSetStruct struct = new PagedDisjointSetStruct(7, AllocationTracker.EMPTY);

    @Before
    public void setup() {
        struct.reset();
    }

    @Test
    public void testSetUnion() throws Exception {

        // {0}{1}{2}{3}{4}{5}{6}
        assertFalse(struct.connected(0, 1));
        assertEquals(7, struct.getSetSize().size());

        struct.union(0, 1);
        // {0,1}{2}{3}{4}{5}{6}
        assertTrue(struct.connected(0, 1));
        assertFalse(struct.connected(2, 3));
        assertEquals(6, struct.getSetSize().size());

        struct.union(2, 3);
        // {0,1}{2,3}{4}{5}{6}
        assertTrue(struct.connected(2, 3));
        assertFalse(struct.connected(0, 2));
        assertFalse(struct.connected(0, 3));
        assertFalse(struct.connected(1, 2));
        assertFalse(struct.connected(1, 3));
        assertEquals(5, struct.getSetSize().size());

        struct.union(3, 0);
        // {0,1,2,3}{4}{5}{6}
        assertTrue(struct.connected(0, 2));
        assertTrue(struct.connected(0, 3));
        assertTrue(struct.connected(1, 2));
        assertTrue(struct.connected(1, 3));
        assertFalse(struct.connected(4, 5));
        assertEquals(4, struct.getSetSize().size());

        struct.union(4, 5);
        // {0,1,2,3}{4,5}{6}
        assertTrue(struct.connected(4, 5));
        assertFalse(struct.connected(0, 4));
        assertEquals(3, struct.getSetSize().size());

        struct.union(0, 4);
        // {0,1,2,3,4,5}{6}
        assertTrue(struct.connected(0, 4));
        assertTrue(struct.connected(0, 5));
        assertTrue(struct.connected(1, 4));
        assertTrue(struct.connected(1, 5));
        assertTrue(struct.connected(2, 4));
        assertTrue(struct.connected(2, 5));
        assertTrue(struct.connected(3, 4));
        assertTrue(struct.connected(3, 5));
        assertTrue(struct.connected(4, 5));
        assertFalse(struct.connected(0, 6));
        assertFalse(struct.connected(1, 6));
        assertFalse(struct.connected(2, 6));
        assertFalse(struct.connected(3, 6));
        assertFalse(struct.connected(4, 6));
        assertFalse(struct.connected(5, 6));

        final LongLongMap setSize = struct.getSetSize();
        System.out.println(setSize);
        assertEquals(2, setSize.size());
        for (LongLongCursor cursor : setSize) {
            assertTrue(cursor.value == 6 || cursor.value == 1);
        }
    }

    @Test
    public void testDefault() throws Exception {
        PagedDisjointSetStruct.Consumer consumer = mock(PagedDisjointSetStruct.Consumer.class);
        when(consumer.consume(anyLong(), anyLong())).thenReturn(true);
        struct.forEach(consumer);
        verify(consumer, times(7)).consume(anyLong(), anyLong());
        verify(consumer, times(1)).consume(eq(0L), eq(0L));
        verify(consumer, times(1)).consume(eq(1L), eq(1L));
        verify(consumer, times(1)).consume(eq(2L), eq(2L));
        verify(consumer, times(1)).consume(eq(3L), eq(3L));
        verify(consumer, times(1)).consume(eq(4L), eq(4L));
        verify(consumer, times(1)).consume(eq(5L), eq(5L));
        verify(consumer, times(1)).consume(eq(6L), eq(6L));
    }

    @Test
    public void testMergeDSS() throws Exception {
        final PagedDisjointSetStruct a = create(10, set(0, 1, 2, 3), set(4, 5, 6), set(7, 8), set(9));
        final PagedDisjointSetStruct b = create(10, set(0, 5), set(7, 9));
        assertEquals(4, a.getSetCount());
        a.merge(b);
        assertEquals(2, a.getSetCount());
        System.out.println(a);
    }

    @Test
    public void shouldComputeMemoryEstimation() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(MemoryRange.of(112), PagedDisjointSetStruct.memoryEstimation().apply(dimensions0, 1).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(MemoryRange.of(1712), PagedDisjointSetStruct.memoryEstimation().apply(dimensions100, 1).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(MemoryRange.of(1600244140768L), PagedDisjointSetStruct.memoryEstimation().apply(dimensions100B, 1).memoryUsage());
    }

    public static int[] set(int... elements) {
        return elements;
    }

    private static PagedDisjointSetStruct create(int size, int[]... sets) {
        final PagedDisjointSetStruct dss = new PagedDisjointSetStruct(size, AllocationTracker.EMPTY).reset();
        for (int[] set : sets) {
            if (set.length < 1) {
                throw new IllegalArgumentException("Sets must contain at least one element");
            }
            for (int i = 1; i < set.length; i++) {
                dss.union(set[i], set[0]);
            }
        }
        return dss;
    }

    @Test
    public void test() throws Exception {
        System.out.println(new PagedDisjointSetStruct(5, AllocationTracker.EMPTY).reset());
    }
}
