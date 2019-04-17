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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

public final class PagedLongDoubleMapTest {

    @Test
    public void canReadFromPut() {
        PagedLongDoubleMap map = PagedLongDoubleMap.of(4L, AllocationTracker.EMPTY);
        map.put(1L, 1.0);

        double actual = map.getOrDefault(1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        // different key
        actual = map.getOrDefault(2L, 0.0);
        assertEquals(0.0, actual, 1e-4);
    }

    @Test
    public void supportsZeroKeys() {
        PagedLongDoubleMap map = PagedLongDoubleMap.of(4L, AllocationTracker.EMPTY);

        map.put(0L, 1.0);
        double actual = map.getOrDefault(0L, 0.0);
        assertEquals(1.0, actual, 1e-4);
    }

    @Test
    public void acceptsInitialSize() {
        PagedLongDoubleMap map = PagedLongDoubleMap.of(0L, AllocationTracker.EMPTY);
        map.put(1L, 1.0);
        double actual = map.getOrDefault(1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        map = PagedLongDoubleMap.of(1L, AllocationTracker.EMPTY);
        map.put(1L, 1.0);
        actual = map.getOrDefault(1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        map = PagedLongDoubleMap.of(100L, AllocationTracker.EMPTY);
        map.put(1L, 1.0);
        actual = map.getOrDefault(1L, 0.0);
        assertEquals(1.0, actual, 1e-4);
    }

    @Test
    public void resizeOnGrowthAndTrackMemoryUsage() {
        AllocationTracker tracker = AllocationTracker.create();
        PagedLongDoubleMap map = PagedLongDoubleMap.of(0L, tracker);

        long expected = sizeOfObjectArray(0); // empty pages
        assertEquals(expected, tracker.tracked());


        map.put(0L, 1.0);
        expected =
                sizeOfIntArray(9) // keys of 1 map
                        + sizeOfDoubleArray(9)  // values of 1 map
                        + sizeOfObjectArray(1); // 1 page
        assertEquals(expected, tracker.tracked());


        map.put(1L << 14, 2.0);
        expected =
                2L * sizeOfIntArray(9) // keys of 2 maps
                        + 2L * sizeOfDoubleArray(9) // values of 2 maps
                        + sizeOfObjectArray(2);  // 2 pages
        assertEquals(expected, tracker.tracked());


        map.put(3L << 14, 4.0);
        expected =
                3L * sizeOfIntArray(9)  // keys of 3 maps
                        + 3L * sizeOfDoubleArray(9) // values of 3 maps
                        + sizeOfObjectArray(4); // 4 pages, one is null
        assertEquals(expected, tracker.tracked());

        for (long i = 1L; i < 10L; i++) {
            map.put((3L << 14) + i, 4.0 + (double) i);
        }
        expected =
                2L * sizeOfIntArray(9) // keys of 2 maps
                        + sizeOfIntArray(17) // keys of 1 map with more entries
                        + 2L * sizeOfDoubleArray(9) // values of 2 maps
                        + sizeOfDoubleArray(17) // values of 1 map with more entries
                        + sizeOfObjectArray(4); // 4 pages, one is null
        assertEquals(expected, tracker.tracked());
    }

    @Test
    public void releaseMemory() {
        AllocationTracker tracker = AllocationTracker.create();
        PagedLongDoubleMap map = PagedLongDoubleMap.of(4, tracker);

        for (long i = 0L; i < 20L; i++) {
            map.put(i, (double) i * 13.37);
        }
        long tracked = tracker.tracked();
        assertEquals(tracked, map.release());
        assertEquals(0L, tracker.tracked());
    }
}
