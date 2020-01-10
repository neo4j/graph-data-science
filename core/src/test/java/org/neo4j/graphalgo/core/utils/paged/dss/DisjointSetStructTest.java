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
package org.neo4j.graphalgo.core.utils.paged.dss;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DisjointSetStructTest {

    private DisjointSetStruct struct;

    private static final int CAPACITY = 7;

    abstract DisjointSetStruct newSet(int capacity);

    @BeforeEach
    final void setup() {
        struct = newSet(CAPACITY);
    }

    @Test
    final void testSetUnion() {
        // {0}{1}{2}{3}{4}{5}{6}
        assertFalse(connected(struct,0, 1));
        assertEquals(7, getSetSize(struct).size());
        assertEquals(0, struct.setIdOf(0));
        assertEquals(1, struct.setIdOf(1));
        assertEquals(2, struct.setIdOf(2));
        assertEquals(3, struct.setIdOf(3));
        assertEquals(4, struct.setIdOf(4));
        assertEquals(5, struct.setIdOf(5));
        assertEquals(6, struct.setIdOf(6));

        struct.union(0, 1);
        // {0,1}{2}{3}{4}{5}{6}
        assertTrue(connected(struct,0, 1));
        assertFalse(connected(struct,2, 3));
        assertEquals(6, getSetSize(struct).size());

        struct.union(2, 3);
        // {0,1}{2,3}{4}{5}{6}
        assertTrue(connected(struct, 2, 3));
        assertFalse(connected(struct, 0, 2));
        assertFalse(connected(struct, 0, 3));
        assertFalse(connected(struct, 1, 2));
        assertFalse(connected(struct, 1, 3));
        assertEquals(5, getSetSize(struct).size());

        struct.union(3, 0);
        // {0,1,2,3}{4}{5}{6}
        assertTrue(connected(struct, 0, 2));
        assertTrue(connected(struct, 0, 3));
        assertTrue(connected(struct, 1, 2));
        assertTrue(connected(struct, 1, 3));
        assertFalse(connected(struct, 4, 5));
        assertEquals(4, getSetSize(struct).size());

        struct.union(4, 5);
        // {0,1,2,3}{4,5}{6}
        assertTrue(connected(struct, 4, 5));
        assertFalse(connected(struct, 0, 4));
        assertEquals(3, getSetSize(struct).size());

        struct.union(0, 4);
        // {0,1,2,3,4,5}{6}
        assertTrue(connected(struct, 0, 4));
        assertTrue(connected(struct, 0, 5));
        assertTrue(connected(struct, 1, 4));
        assertTrue(connected(struct, 1, 5));
        assertTrue(connected(struct, 2, 4));
        assertTrue(connected(struct, 2, 5));
        assertTrue(connected(struct, 3, 4));
        assertTrue(connected(struct, 3, 5));
        assertTrue(connected(struct, 4, 5));
        assertFalse(connected(struct, 0, 6));
        assertFalse(connected(struct, 1, 6));
        assertFalse(connected(struct, 2, 6));
        assertFalse(connected(struct, 3, 6));
        assertFalse(connected(struct, 4, 6));
        assertFalse(connected(struct, 5, 6));

        assertEquals(struct.setIdOf(0), struct.setIdOf(1));
        assertEquals(struct.setIdOf(0), struct.setIdOf(2));
        assertEquals(struct.setIdOf(0), struct.setIdOf(3));
        assertEquals(struct.setIdOf(0), struct.setIdOf(4));
        assertEquals(struct.setIdOf(0), struct.setIdOf(5));
        assertNotEquals(struct.setIdOf(0), struct.setIdOf(6));

        final LongLongMap setSize = getSetSize(struct);
        assertEquals(2, setSize.size());
        for (LongLongCursor cursor : setSize) {
            assertTrue(cursor.value == 6 || cursor.value == 1);
        }
    }

    @Test
    final void testMergeDSS() {
        final DisjointSetStruct a = create(10, set(0, 1, 2, 3), set(4, 5, 6), set(7, 8), set(9));
        final DisjointSetStruct b = create(10, set(0, 5), set(7, 9));
        if (a instanceof SequentialDisjointSetStruct) {
            SequentialDisjointSetStruct dssa = (SequentialDisjointSetStruct) a;
            if (b instanceof SequentialDisjointSetStruct) {
                SequentialDisjointSetStruct dssb = (SequentialDisjointSetStruct) b;
                assertEquals(4, getSetCount(dssa));
                dssa.merge(dssb);
                assertEquals(2, getSetCount(dssa));

            }
        }
    }

    private static int[] set(int... elements) {
        return elements;
    }

    void assertMemoryEstimation(MemoryEstimation memoryEstimation, long nodeCount, MemoryRange memoryRange) {
        assertMemoryEstimation(memoryEstimation, nodeCount, 1, memoryRange);
    }

    void assertMemoryEstimation(
            final MemoryEstimation memoryEstimation,
            long nodeCount,
            int concurrency,
            MemoryRange memoryRange) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        assertEquals(memoryRange.min, memoryEstimation.estimate(dimensions, concurrency).memoryUsage().min);
    }

    /**
     * Check if p and q belong to the same set.
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    private boolean connected(DisjointSetStruct struct, long p, long q) {
        return struct.sameSet(p, q);
    }

    /**
     * Compute number of sets present.
     */
    long getSetCount(DisjointSetStruct struct) {
        long capacity = struct.size();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = struct.setIdOf(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

    /**
     * Compute the size of each set.
     *
     * @return a map which maps setId to setSize
     */
    private LongLongMap getSetSize(DisjointSetStruct struct) {
        final LongLongMap map = new LongLongScatterMap();
        for (long i = struct.size() - 1; i >= 0; i--) {
            map.addTo(struct.setIdOf(i), 1);
        }
        return map;
    }

    private DisjointSetStruct create(int size, int[]... sets) {
        DisjointSetStruct dss = newSet(size);
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
}
