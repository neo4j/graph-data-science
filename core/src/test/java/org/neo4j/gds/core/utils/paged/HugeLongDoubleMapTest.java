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

import com.carrotsearch.hppc.LongDoubleHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;

final class HugeLongDoubleMapTest {

    @Test
    void canClear() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(AllocationTracker.empty());
        map.addTo(1, 1);
        map.clear();
        assertEquals(0, map.size());
        map.addTo(1, 23);
        map.addTo(2, 24);
        assertEquals(2, map.size());
        assertEquals(23, map.getOrDefault(1, 42));
        assertEquals(24, map.getOrDefault(2, 42));
    }

    @Test
    void canReadFromAddTo() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(AllocationTracker.empty());
        map.addTo(1L, 1);

        double actual = map.getOrDefault(1L, 0);
        assertEquals(1L, actual);

        // different key
        actual = map.getOrDefault(2L, 0L);
        assertEquals(0L, actual);
    }

    @Test
    void addToAddsValues() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(AllocationTracker.empty());
        map.addTo(1L, 1L);
        map.addTo(1L, 2L);
        map.addTo(1L, 3L);
        map.addTo(1L, 4L);

        double actual = map.getOrDefault(1L, 0L);
        assertEquals(10L, actual);
    }

    @Test
    void acceptsInitialSize() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(0L, AllocationTracker.empty());
        map.addTo(1L, 1L);
        double actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);

        map = new HugeLongDoubleMap(1L, AllocationTracker.empty());
        map.addTo(1L, 1L);
        actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);

        map = new HugeLongDoubleMap(100L, AllocationTracker.empty());
        map.addTo(1L, 1L);
        actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);
    }

    @Test
    void hasSize() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(AllocationTracker.empty());
        assertEquals(0L, map.size());

        map.addTo(1L, 1L);
        assertEquals(1L, map.size());

        map.addTo(2L, 2L);
        assertEquals(2L, map.size());

        // same key
        map.addTo(1L, 2L);
        assertEquals(2L, map.size());
    }

    @Test
    void hasIsEmpty() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(AllocationTracker.empty());
        assertTrue(map.isEmpty());
        map.addTo(1L, 1L);
        assertFalse(map.isEmpty());
    }

    @Test
    void resizeOnGrowthAndTrackMemoryUsage() {
        long firstSize = sizeOfLongArray(8) + sizeOfDoubleArray(8);
        long secondSize = sizeOfLongArray(16) + sizeOfDoubleArray(16);
        long thirdSize = sizeOfLongArray(32) + sizeOfDoubleArray(32);

        AllocationTracker allocationTracker = AllocationTracker.create();
        HugeLongDoubleMap map = new HugeLongDoubleMap(allocationTracker);

        for (long i = 0L; i < 6L; i++) {
            map.addTo(i, i + 42L);
            assertEquals(firstSize, allocationTracker.trackedBytes());
        }
        for (long i = 6L; i < 12L; i++) {
            map.addTo(i, i + 42L);
            assertEquals(secondSize, allocationTracker.trackedBytes());
        }
        for (long i = 12L; i < 24L; i++) {
            map.addTo(i, i + 42L);
            assertEquals(thirdSize, allocationTracker.trackedBytes());
        }
    }

    @Test
    void releaseMemory() {
        AllocationTracker allocationTracker = AllocationTracker.create();
        HugeLongDoubleMap map = new HugeLongDoubleMap(allocationTracker);

        for (long i = 0L; i < 20L; i++) {
            map.addTo(i, i + 42L);
        }
        map.release();
        assertEquals(0L, allocationTracker.trackedBytes());
    }

    @Test
    void hasStringRepresentation() {
        HugeLongDoubleMap map = new HugeLongDoubleMap(AllocationTracker.empty());
        LongDoubleHashMap compare = new LongDoubleHashMap();

        assertEquals("[]", map.toString());

        for (long i = 0L; i < 20L; i++) {
            map.addTo(i, i + 42L);
            compare.put(i, i + 42L);
        }

        // order is different, need to fake sort
        assertEquals(sortedToString(compare.toString()), sortedToString(map.toString()));
    }

    private static final Pattern COMMA_WS = Pattern.compile(", ");
    private static final Pattern ARROW = Pattern.compile("=>");
    private static final Pattern NON_DIGITS = Pattern.compile("\\D+");

    private static String sortedToString(String out) {
        return COMMA_WS.splitAsStream(out.substring(1, out.length() - 1))
                .sorted(HugeLongDoubleMapTest::comparePrEntry)
                .collect(Collectors.joining(", "));
    }

    private static int comparePrEntry(String key1, String key2) {
        double[] keys1 = getKeyPair(key1);
        double[] keys2 = getKeyPair(key2);
        for (int i = 0; i < keys1.length; i++) {
            int compare = Double.compare(keys1[i], keys2[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private static double[] getKeyPair(String entry) {
        return ARROW.splitAsStream(entry)
                .limit(1L)
                .flatMap(NON_DIGITS::splitAsStream)
                .filter(s -> !s.isEmpty())
                .mapToDouble(Double::parseDouble)
                .toArray();
    }
}
