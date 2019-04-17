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
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import org.junit.Test;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;

public final class HugeLongLongDoubleMapTest {

    @Test
    public void canReadFromAddTo() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);
        map.addTo(1L, 1L, 1.0);

        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        // different first key
        actual = map.getOrDefault(2L, 1L, 0.0);
        assertEquals(0.0, actual, 1e-4);

        // different second key
        actual = map.getOrDefault(1L, 2L, 0.0);
        assertEquals(0.0, actual, 1e-4);

        // different keys
        actual = map.getOrDefault(2L, 2L, 0.0);
        assertEquals(0.0, actual, 1e-4);
    }

    @Test
    public void supportsTwoKeys() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);
        map.addTo(1L, 1L, 1.0);
        map.addTo(1L, 2L, 2.0);
        map.addTo(2L, 1L, 3.0);
        map.addTo(2L, 2L, 4.0);

        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        actual = map.getOrDefault(1L, 2L, 0.0);
        assertEquals(2.0, actual, 1e-4);

        actual = map.getOrDefault(2L, 1L, 0.0);
        assertEquals(3.0, actual, 1e-4);

        actual = map.getOrDefault(2L, 2L, 0.0);
        assertEquals(4.0, actual, 1e-4);
    }

    @Test
    public void supportsZeroKeys() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);

        map.addTo(0L, 0L, 1.0);
        double actual = map.getOrDefault(0L, 0L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        map.addTo(0L, 2L, 2.0);
        actual = map.getOrDefault(0L, 0L, 0.0);
        assertEquals(1.0, actual, 1e-4);
        actual = map.getOrDefault(0L, 2L, 0.0);
        assertEquals(2.0, actual, 1e-4);

        map.addTo(2L, 0L, 4.0);
        actual = map.getOrDefault(0L, 0L, 0.0);
        assertEquals(1.0, actual, 1e-4);
        actual = map.getOrDefault(0L, 2L, 0.0);
        assertEquals(2.0, actual, 1e-4);
        actual = map.getOrDefault(2L, 0L, 0.0);
        assertEquals(4.0, actual, 1e-4);
    }

    @Test
    public void addToAddsValues() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);
        map.addTo(1L, 1L, 1.0);
        map.addTo(1L, 1L, 2.0);
        map.addTo(1L, 1L, 3.0);
        map.addTo(1L, 1L, 4.0);

        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(10.0, actual, 1e-4);
    }

    @Test
    public void acceptsInitialSize() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(0L, AllocationTracker.EMPTY);
        map.addTo(1L, 1L, 1.0);
        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        map = new HugeLongLongDoubleMap(1L, AllocationTracker.EMPTY);
        map.addTo(1L, 1L, 1.0);
        actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        map = new HugeLongLongDoubleMap(100L, AllocationTracker.EMPTY);
        map.addTo(1L, 1L, 1.0);
        actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);
    }

    @Test
    public void hasSize() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);
        assertEquals(0L, map.size());

        map.addTo(1L, 1L, 1.0);
        assertEquals(1L, map.size());

        map.addTo(2L, 2L, 2.0);
        assertEquals(2L, map.size());

        // same key
        map.addTo(1L, 1L, 2.0);
        assertEquals(2L, map.size());
    }

    @Test
    public void hasIsEmpty() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);
        assertTrue(map.isEmpty());
        map.addTo(1L, 1L, 1.0);
        assertFalse(map.isEmpty());
    }

    @Test
    public void resizeOnGrowthAndTrackMemoryUsage() {
        long firstSize = 2L * sizeOfLongArray(8) + sizeOfDoubleArray(8);
        long secondSize = 2L * sizeOfLongArray(16) + sizeOfDoubleArray(16);
        long thirdSize = 2L * sizeOfLongArray(32) + sizeOfDoubleArray(32);

        AllocationTracker tracker = AllocationTracker.create();
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(tracker);

        for (long i = 0L; i < 6L; i++) {
            map.addTo(i, i * 42L, (double) i * 13.37);
            assertEquals(firstSize, tracker.tracked());
        }
        for (long i = 6L; i < 12L; i++) {
            map.addTo(i, i * 42L, (double) i * 13.37);
            assertEquals(secondSize, tracker.tracked());
        }
        for (long i = 12L; i < 24L; i++) {
            map.addTo(i, i * 42L, (double) i * 13.37);
            assertEquals(thirdSize, tracker.tracked());
        }
    }

    @Test
    public void releaseMemory() {
        AllocationTracker tracker = AllocationTracker.create();
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(tracker);

        for (long i = 0L; i < 20L; i++) {
            map.addTo(i, i * 42L, (double) i * 13.37);
        }
        map.release();
        assertEquals(0L, tracker.tracked());
    }

    @Test
    public void hasStringRepresentation() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(AllocationTracker.EMPTY);
        ObjectDoubleHashMap<Pr> compare = new ObjectDoubleHashMap<>();

        assertEquals("[]", map.toString());

        for (long i = 0L; i < 20L; i++) {
            map.addTo(i, i * 42L, (double) i * 13.37);
            compare.put(new Pr(i, i * 42L), (double) i * 13.37);
        }

        // order is different, need to fake sort
        assertEquals(sortedToString(compare.toString()), sortedToString(map.toString()));
    }

    private static final Pattern COMMA_WS = Pattern.compile(", ");
    private static final Pattern ARROW = Pattern.compile("=>");
    private static final Pattern NON_DIGITS = Pattern.compile("\\D+");

    private static String sortedToString(String out) {
        return COMMA_WS.splitAsStream(out.substring(1, out.length() - 1))
                .sorted(HugeLongLongDoubleMapTest::comparePrEntry)
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

    static final class Pr {
        long k1;
        long k2;

        Pr(final long k1, final long k2) {
            this.k1 = k1;
            this.k2 = k2;
        }

        @Override
        public String toString() {
            return String.format("(%d,%d)", k1, k2);
        }

        @Override
        public int hashCode() {
            return (int) (k1 ^ k2);
        }
    }
}
