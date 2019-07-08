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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.core.utils.paged.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.RankedDisjointSetStruct;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public abstract class DisjointSetStructTest {

    private DisjointSetStruct struct;

    private static final int CAPACITY = 7;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{new TestWeightMapping(), "Empty"},
                new Object[]{new TestWeightMapping(IntStream.range(0, CAPACITY).flatMap(i -> IntStream.of(i, i)).toArray()), "Ordered"},
                new Object[]{new TestWeightMapping(IntStream.range(0, CAPACITY).flatMap(i -> IntStream.of(i, CAPACITY - i - 1)).toArray()), "Reversed"}
        );
    }

    @Parameterized.Parameter()
    public TestWeightMapping weightMapping;

    @Parameterized.Parameter(1)
    public String name;

    abstract DisjointSetStruct newSet(int capacity);

    abstract DisjointSetStruct newSet(int capacity, TestWeightMapping weightMapping);

    @Before
    public final void setup() {
        struct = newSet(CAPACITY, weightMapping).reset();
    }

    @Test
    public final void testInitialCommunities() {
        Assume.assumeFalse(struct instanceof RankedDisjointSetStruct);

        // {0,1}{2,3}{4}{5}{6}
        DisjointSetStruct localDss = newSet(
                CAPACITY,
                new TestWeightMapping(0, 0, 1, 0, 2, 1, 3, 1, 4, 4, 5, 5, 6, 6))
                .reset();

        assertTrue(localDss.connected(2, 3));
        assertFalse(localDss.connected(0, 2));
        assertFalse(localDss.connected(0, 3));
        assertFalse(localDss.connected(1, 2));
        assertFalse(localDss.connected(1, 3));
        assertEquals(5, localDss.getSetSize().size());

        localDss.union(3, 0);
        // {0,1,2,3}{4}{5}{6}
        assertTrue(localDss.connected(0, 2));
        assertTrue(localDss.connected(0, 3));
        assertTrue(localDss.connected(1, 2));
        assertTrue(localDss.connected(1, 3));
        assertFalse(localDss.connected(4, 5));
        assertEquals(4, localDss.getSetSize().size());

        localDss.union(4, 5);
        // {0,1,2,3}{4,5}{6}
        assertTrue(localDss.connected(4, 5));
        assertFalse(localDss.connected(0, 4));
        assertEquals(3, localDss.getSetSize().size());

        localDss.union(0, 4);
        // {0,1,2,3,4,5}{6}
        assertTrue(localDss.connected(0, 4));
        assertTrue(localDss.connected(0, 5));
        assertTrue(localDss.connected(1, 4));
        assertTrue(localDss.connected(1, 5));
        assertTrue(localDss.connected(2, 4));
        assertTrue(localDss.connected(2, 5));
        assertTrue(localDss.connected(3, 4));
        assertTrue(localDss.connected(3, 5));
        assertTrue(localDss.connected(4, 5));
        assertFalse(localDss.connected(0, 6));
        assertFalse(localDss.connected(1, 6));
        assertFalse(localDss.connected(2, 6));
        assertFalse(localDss.connected(3, 6));
        assertFalse(localDss.connected(4, 6));
        assertFalse(localDss.connected(5, 6));

        final LongLongMap setSize = localDss.getSetSize();
        assertEquals(2, setSize.size());
        for (LongLongCursor cursor : setSize) {
            assertTrue(cursor.value == 6 || cursor.value == 1);
        }

    }

    @Test
    public final void testSetUnion() {

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
        assertEquals(2, setSize.size());
        for (LongLongCursor cursor : setSize) {
            assertTrue(cursor.value == 6 || cursor.value == 1);
        }
    }

    @Test
    public final void testMergeDSS() {
        final DisjointSetStruct a = create(10, set(0, 1, 2, 3), set(4, 5, 6), set(7, 8), set(9));
        final DisjointSetStruct b = create(10, set(0, 5), set(7, 9));
        assertEquals(4, a.getSetCount());
        a.merge(b);
        assertEquals(2, a.getSetCount());
    }


    public static int[] set(int... elements) {
        return elements;
    }

    private DisjointSetStruct create(int size, int[]... sets) {
        DisjointSetStruct dss = newSet(size).reset();
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
