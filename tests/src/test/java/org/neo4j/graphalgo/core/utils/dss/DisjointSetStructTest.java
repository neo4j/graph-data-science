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

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.huge.loader.HugeNodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public abstract class DisjointSetStructTest {

    private DisjointSetStruct struct;

    private static final int CAPACITY = 7;

    abstract DisjointSetStruct newSet(int capacity);

    @Before
    public final void setup() {
        struct = newSet(CAPACITY);
    }

    long maxCommunity = 3;

    @Test
    public void customMapping() {

        long nodeCount = 10_000L;

        final AllocationTracker tracker = AllocationTracker.EMPTY;
        final HugeNodePropertiesBuilder builder = HugeNodePropertiesBuilder.of(
                nodeCount,
                tracker,
                -1.0,
                0);

        final List<Long> longs = LongStream.range(0, nodeCount).boxed().collect(Collectors.toList());
        Collections.shuffle(longs);
        longs.parallelStream().forEach(i -> builder.set(i, (long) (Math.random() * nodeCount)));

        System.out.println(String.format("Max hat Gebuuuuuuuuurtstag"));

        final HugeWeightMapping communityMapping = builder.build();

        this.maxCommunity = LongStream
                .range(0, nodeCount)
                .map(id -> (long) communityMapping.nodeWeight(id, -1))
                .max().orElse(0);
        System.out.println(String.format("Max community id (before init): %d", maxCommunity));

        HugeLongArray parent = HugeLongArray.newArray(nodeCount, tracker);
        LongLongHashMap internalToProvidedIds = new LongLongHashMap();
        internalToProvidedIds.clear();


        final LongLongMap internalMapping = new LongLongHashMap();

        parent.setAll(nodeId -> {
            long parentValue = -1;
            double communityIdValue = communityMapping.nodeWeight(nodeId, Double.NaN);

            if (!Double.isNaN(communityIdValue)) {
                long communityId = (long) communityIdValue;

                int idIndex = internalMapping.indexOf(communityId);
                if (internalMapping.indexExists(idIndex)) {
                    parentValue = internalMapping.indexGet(idIndex);
                } else {
                    internalToProvidedIds.put(nodeId, communityId);
                    internalMapping.indexInsert(idIndex, communityId, nodeId);
                }
            } else {
                internalToProvidedIds.put(nodeId, ++maxCommunity);
            }
            return parentValue;
        });

        System.out.println(String.format("Internal mapping size: %d", internalMapping.size()));
        System.out.println(String.format("Internal to provided ids size: %d", internalToProvidedIds.size()));
        System.out.println(String.format("Max community id (after init): %d", maxCommunity));
    }

    @Test
    public void bitshiftTests() {
        System.out.println(2 << 1);
        System.out.println(2 << 2);
        System.out.println(2 << 3);

        System.out.println(BitUtil.previousPowerOfTwo(10));
    }

    @Test
    public final void testSetUnion() {
        // {0}{1}{2}{3}{4}{5}{6}
        assertFalse(struct.connected(0, 1));
        assertEquals(7, struct.getSetSize().size());
        assertEquals(0, struct.setIdOf(0));
        assertEquals(1, struct.setIdOf(1));
        assertEquals(2, struct.setIdOf(2));
        assertEquals(3, struct.setIdOf(3));
        assertEquals(4, struct.setIdOf(4));
        assertEquals(5, struct.setIdOf(5));
        assertEquals(6, struct.setIdOf(6));

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

        assertEquals(struct.setIdOf(0), struct.setIdOf(1));
        assertEquals(struct.setIdOf(0), struct.setIdOf(2));
        assertEquals(struct.setIdOf(0), struct.setIdOf(3));
        assertEquals(struct.setIdOf(0), struct.setIdOf(4));
        assertEquals(struct.setIdOf(0), struct.setIdOf(5));
        assertNotEquals(struct.setIdOf(0), struct.setIdOf(6));

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
