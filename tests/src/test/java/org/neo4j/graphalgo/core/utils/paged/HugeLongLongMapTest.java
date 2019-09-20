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
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.LongLongHashMap;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;

final class HugeLongLongMapTest {

    @Test
    void canReadFromAddTo() {
        HugeLongLongMap map = new HugeLongLongMap(AllocationTracker.EMPTY);
        map.addTo(1L, 1L);

        long actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);

        // different key
        actual = map.getOrDefault(2L, 0L);
        assertEquals(0L, actual);
    }

    @Test
    void supportsNullKeys() {
        HugeLongLongMap map = new HugeLongLongMap(AllocationTracker.EMPTY);

        map.addTo(0L, 1L);
        long actual = map.getOrDefault(0L, 0L);
        assertEquals(1L, actual);

        map.addTo(0L, 2L);
        actual = map.getOrDefault(0L, 0L);
        assertEquals(3L, actual);
    }

    @Test
    void addToAddsValues() {
        HugeLongLongMap map = new HugeLongLongMap(AllocationTracker.EMPTY);
        map.addTo(1L, 1L);
        map.addTo(1L, 2L);
        map.addTo(1L, 3L);
        map.addTo(1L, 4L);

        long actual = map.getOrDefault(1L, 0L);
        assertEquals(10L, actual);
    }

    @Test
    void acceptsInitialSize() {
        HugeLongLongMap map = new HugeLongLongMap(0L, AllocationTracker.EMPTY);
        map.addTo(1L, 1L);
        long actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);

        map = new HugeLongLongMap(1L, AllocationTracker.EMPTY);
        map.addTo(1L, 1L);
        actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);

        map = new HugeLongLongMap(100L, AllocationTracker.EMPTY);
        map.addTo(1L, 1L);
        actual = map.getOrDefault(1L, 0L);
        assertEquals(1L, actual);
    }

    @Test
    void hasSize() {
        HugeLongLongMap map = new HugeLongLongMap(AllocationTracker.EMPTY);
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
        HugeLongLongMap map = new HugeLongLongMap(AllocationTracker.EMPTY);
        assertTrue(map.isEmpty());
        map.addTo(1L, 1L);
        assertFalse(map.isEmpty());
    }

    @Test
    void resizeOnGrowthAndTrackMemoryUsage() {
        long firstSize = sizeOfLongArray(8) + sizeOfDoubleArray(8);
        long secondSize = sizeOfLongArray(16) + sizeOfDoubleArray(16);
        long thirdSize = sizeOfLongArray(32) + sizeOfDoubleArray(32);

        AllocationTracker tracker = AllocationTracker.create();
        HugeLongLongMap map = new HugeLongLongMap(tracker);

        for (long i = 0L; i < 6L; i++) {
            map.addTo(i, i + 42L);
            assertEquals(firstSize, tracker.tracked());
        }
        for (long i = 6L; i < 12L; i++) {
            map.addTo(i, i + 42L);
            assertEquals(secondSize, tracker.tracked());
        }
        for (long i = 12L; i < 24L; i++) {
            map.addTo(i, i + 42L);
            assertEquals(thirdSize, tracker.tracked());
        }
    }

    @Test
    void releaseMemory() {
        AllocationTracker tracker = AllocationTracker.create();
        HugeLongLongMap map = new HugeLongLongMap(tracker);

        for (long i = 0L; i < 20L; i++) {
            map.addTo(i, i + 42L);
        }
        map.release();
        assertEquals(0L, tracker.tracked());
    }

    @Test
    void hasStringRepresentation() {
        HugeLongLongMap map = new HugeLongLongMap(AllocationTracker.EMPTY);
        LongLongHashMap compare = new LongLongHashMap();

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
                .sorted(HugeLongLongMapTest::comparePrEntry)
                .collect(Collectors.joining(", "));
    }

    private static int comparePrEntry(String key1, String key2) {
        int[] keys1 = getKeyPair(key1);
        int[] keys2 = getKeyPair(key2);
        for (int i = 0; i < keys1.length; i++) {
            int compare = Integer.compare(keys1[i], keys2[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private static int[] getKeyPair(String entry) {
        return ARROW.splitAsStream(entry)
                .limit(1L)
                .flatMap(NON_DIGITS::splitAsStream)
                .filter(s -> !s.isEmpty())
                .mapToInt(Integer::parseInt)
                .toArray();
    }
}
