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
package org.neo4j.gds.core.utils.paged.dss;

import com.carrotsearch.hppc.IntIntHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.core.utils.paged.dss.IncrementalDisjointSetStruct.memoryEstimation;

class IncrementalDisjointSetStructTest extends DisjointSetStructTest {

    @Override
    SequentialDisjointSetStruct newSet(final int capacity) {
        NodeProperties communities = new TestNodeProperties();
        return newSet(capacity, communities);
    }

    SequentialDisjointSetStruct newSet(final int capacity, final NodeProperties weightMapping) {
        return new IncrementalDisjointSetStruct(capacity, weightMapping, AllocationTracker.empty());
    }

    @Test
    void shouldRunWithLessInitialCommunities() {
        NodeProperties communities = new TestNodeProperties(0, 0, 1, 0);
        SequentialDisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, getSetCount(dss));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    void shouldRunWithLessInitialCommunitiesAndLargerIdSpace() {
        NodeProperties communities = new TestNodeProperties(0, 10, 1, 10);
        SequentialDisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, getSetCount(dss));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    void shouldRunWithLessInitialCommunitiesAndOverlappingIdSpace() {
        NodeProperties communities = new TestNodeProperties(0, 3, 1, 3);
        SequentialDisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, getSetCount(dss));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    void shouldComputeMemoryEstimation() {
        assertMemoryEstimation(memoryEstimation(), 0, MemoryRange.of(296));
        assertMemoryEstimation(memoryEstimation(), 100, MemoryRange.of(2696));
        assertMemoryEstimation(memoryEstimation(), 100_000_000_000L, MemoryRange.of(2_400_366_211_280L));
    }

    static final class TestNodeProperties implements LongNodeProperties {
        private final IntIntHashMap weights;

        private TestNodeProperties(final IntIntHashMap weights) {
            this.weights = weights;
        }

        TestNodeProperties(int... values) {
            this(toMap(values));
        }

        private static IntIntHashMap toMap(int... values) {
            assert values.length % 2 == 0;
            IntIntHashMap map = new IntIntHashMap(values.length / 2);
            for (int i = 0; i < values.length; i += 2) {
                int key = values[i];
                int value = values[i + 1];
                map.put(key, value);
            }
            return map;
        }

        @Override
        public long longValue(long nodeId) {
            int key = Math.toIntExact(nodeId);
            int index = weights.indexOf(key);
            if (weights.indexExists(index)) {
                return weights.indexGet(index);
            }
            return DefaultValue.LONG_DEFAULT_FALLBACK;
        }

        @Override
        public OptionalLong getMaxLongPropertyValue() {
            return StreamSupport
                .stream(weights.values().spliterator(), false)
                .mapToLong(d -> d.value)
                .max();
        }

        @Override
        public OptionalDouble getMaxDoublePropertyValue() {
            return getMaxLongPropertyValue().stream().mapToDouble(l -> Long.valueOf(l).doubleValue()).findFirst();
        }

        @Override
        public long size() {
            return weights.size();
        }
    }
}
