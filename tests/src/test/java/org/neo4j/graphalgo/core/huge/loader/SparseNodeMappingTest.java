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
package org.neo4j.graphalgo.core.huge.loader;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.SparseNodeMapping;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.PrivateLookup;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.qala.datagen.RandomShortApi.integer;
import static io.qala.datagen.RandomValue.between;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

final class SparseNodeMappingTest {

    private static final int PS = PageUtil.pageSizeFor(Long.BYTES);
    private static final MethodHandle PAGES = PrivateLookup.field(SparseNodeMapping.Builder.class, AtomicReferenceArray.class, "pages");

    @Test
    void shouldSetAndGet() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        int index = integer(2, 8);
        int value = integer(42, 1337);
        array.set(index, value);
        assertEquals(value, array.build().get(index));
    }

    @Test
    void shouldSetValuesInPageSizedChunks() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        // larger than the desired size, but still within a single page
        array.set(integer(11, PS - 1), 1337);
        // doesn't fail - good
    }

    // capacity check is only an assert
    @Test
    void shouldUseCapacityInPageSizedChunks() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        // larger than the desired size, but still within a single page
        try {
            array.set(integer(PS, 2 * PS - 1), 1337);
            fail("should have failed out of capacity");
        } catch (AssertionError | ArrayIndexOutOfBoundsException ignore) {
        }
    }

    @Test
    void shouldHaveContains() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        int index = integer(2, 8);
        int value = integer(42, 1337);
        array.set(index, value);
        SparseNodeMapping longArray = array.build();
        for (int i = 0; i < 10; i++) {
            if (i == index) {
                assertTrue(longArray.contains(i), "Expected index " + i + " to be contained, but it was not");
            } else {
                assertFalse(longArray.contains(i), "Expected index " + i + " not to be contained, but it actually was");
            }
        }
    }

    @Test
    void shouldReturnNegativeOneForMissingValues() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        int index = integer(2, 8);
        int value = integer(42, 1337);
        array.set(index, value);
        SparseNodeMapping longArray = array.build();
        for (int i = 0; i < 10; i++) {
            boolean expectedContains = i == index;
            long actual = longArray.get(i);
            String message = expectedContains
                    ? "Expected value at index " + i + " to be set to " + value + ", but got " + actual + " instead"
                    : "Expected value at index " + i + " to be missing (-1), but got " + actual + " instead";
            assertEquals(expectedContains ? value : -1L, actual, message);
        }
    }

    @Test
    void shouldReturnNegativeOneForOutOfRangeValues() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        array.set(integer(2, 8), integer(42, 1337));
        assertEquals(-1L, array.build().get(integer(100, 200)));
    }

    @Test
    void shouldReturnNegativeOneForUnsetPages() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(PS + 10, AllocationTracker.EMPTY);
        array.set(5000, integer(42, 1337));
        assertEquals(-1L, array.build().get(integer(100, 200)));
    }

    @Test
    void shouldNotFailOnGetsThatAreoutOfBounds() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        array.set(integer(2, 8), integer(42, 1337));
        assertEquals(-1L, array.build().get(integer(100_000_000, 200_000_000)));
        // doesn't fail - good
    }

    @Test
    void shouldNotCreateAnyPagesOnInitialization() throws Throwable {
        AllocationTracker tracker = AllocationTracker.create();
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(2 * PS, tracker);

        final AtomicReferenceArray<long[]> pages = getPages(array);
        for (int i = 0; i < pages.length(); i++) {
            long[] page = pages.get(i);
            assertNull(page);
        }

        long tracked = tracker.tracked();
        // allow some bytes for the container type and
        assertEquals(sizeOfObjectArray(2), tracked);
    }

    @Test
    void shouldCreateAndTrackSparsePagesOnDemand() throws Throwable {
        AllocationTracker tracker = AllocationTracker.create();
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(2 * PS, tracker);

        int index = integer(PS, 2 * PS - 1);
        int value = integer(42, 1337);
        array.set(index, value);

        final AtomicReferenceArray<long[]> pages = getPages(array);
        for (int i = 0; i < pages.length(); i++) {
            if (i == 1) {
                assertNotNull(pages.get(i));
            } else {
                assertNull(pages.get(i));
            }
        }

        long tracked = tracker.tracked();
        assertEquals(sizeOfObjectArray(2) + sizeOfLongArray(PS), tracked);
    }

    @Test
    void shouldComputeMemoryEstimationForBestCase() {
        long size = integer(Integer.MAX_VALUE);
        final MemoryRange memoryRange = SparseNodeMapping.memoryEstimation(size, size);
        assertEquals(memoryRange.min, memoryRange.max);
    }

    @Test
    void shouldComputeMemoryEstimationForWorstCase() {
        long size = integer(Integer.MAX_VALUE);
        long highestId = between(size + 4096L, size * 4096L).Long();
        final MemoryRange memoryRange = SparseNodeMapping.memoryEstimation(highestId, size);
        assertTrue(memoryRange.min < memoryRange.max);
    }

    @Test
    void shouldComputeMemoryEstimation() {
        assertEquals(MemoryRange.of(40L), SparseNodeMapping.memoryEstimation(0L, 0L));
        assertEquals(MemoryRange.of(32832L), SparseNodeMapping.memoryEstimation(100L, 100L));
        assertEquals(MemoryRange.of(97_689_080L), SparseNodeMapping.memoryEstimation(100_000_000_000L, 1L));
        assertEquals(MemoryRange.of(177_714_824L, 327_937_656_296L), SparseNodeMapping.memoryEstimation(100_000_000_000L, 10_000_000L));
        assertEquals(MemoryRange.of(898_077_656L, 800_488_297_688L), SparseNodeMapping.memoryEstimation(100_000_000_000L, 100_000_000L));
    }

    @Test
    void shouldComputeMemoryEstimationDocumented() {
        int pageSize = 4096;
        // int numPagesForSize = PageUtil.numPagesFor(size, PAGE_SHIFT, (int) PAGE_MASK);
        int size = 10_000;
        int numPagesForSize = (int) Math.ceil((double) size / pageSize);

        // int numPagesForMaxEntriesBestCase = PageUtil.numPagesFor(maxEntries, PAGE_SHIFT, (int) PAGE_MASK);
        int maxEntries = 100;
        int numPagesForMaxEntriesBestCase = (int) Math.ceil((double) maxEntries / pageSize);

        // final long maxEntriesForWorstCase = Math.min(size, maxEntries * PAGE_SIZE);
        int maxEntriesWorstCase = Math.min(size, maxEntries * pageSize);
        // int numPagesForMaxEntriesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, PAGE_SHIFT, (int) PAGE_MASK);
        int numPagesForMaxEntriesWorstCase = (int) Math.ceil((double) maxEntriesWorstCase / pageSize);

        //long classSize = MemoryUsage.sizeOfInstance(SparseNodeMapping.class);
        //  private final long capacity;
        //  private final long[][] pages;
        //  private final AllocationTracker tracker;
        long classSizeComponents =
                8L /* capacity */ +
                4L /* ref for long[][] array */ +
                12L /* object header */;

        long classSize = BitUtil.align(classSizeComponents, 8 /* object alignment */);

        // long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);
        //  alignObjectSize((long) BYTES_ARRAY_HEADER + ((long) length << SHIFT_OBJECT_REF));
        long pagesSizeComponents =
                16L /* BYTES_ARRAY_HEADER = object header (8 byte + object ref) + int (length) */ +
                numPagesForSize * 4; /* length << SHIFT_OBJECT_REF (2) */
        long pagesSize = BitUtil.align(pagesSizeComponents, 8 /* object alignment */);

        // long minRequirements = numPagesForMaxEntriesBestCase * PAGE_SIZE_IN_BYTES;
        long minRequirements = numPagesForMaxEntriesBestCase * (4096 * 8 + 16) /* byte array header*/;
        // long maxRequirements = numPagesForMaxEntriesWorstCase * PAGE_SIZE_IN_BYTES;
        long maxRequirements = numPagesForMaxEntriesWorstCase * (4096 * 8 + 16) /* byte array header*/;
        long min = classSize + pagesSize + minRequirements;
        long max = classSize + pagesSize + maxRequirements;

        assertEquals(MemoryRange.of(min, max), SparseNodeMapping.memoryEstimation(size, maxEntries));
    }

    @SuppressWarnings("unchecked")
    private static AtomicReferenceArray<long[]> getPages(SparseNodeMapping.Builder array) throws Throwable {
        return (AtomicReferenceArray<long[]>) PAGES.invoke(array);
    }
}
