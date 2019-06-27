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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.PrivateLookup;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;


@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public final class SparseNodeMappingTest extends RandomizedTest {

    private static final int PS = PageUtil.pageSizeFor(Long.BYTES);
    private static final MethodHandle PAGES = PrivateLookup.field(SparseNodeMapping.Builder.class, AtomicReferenceArray.class, "pages");

    @Test
    public void shouldSetAndGet() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        int index = between(2, 8);
        int value = between(42, 1337);
        array.set(index, value);
        assertEquals(value, array.build().get(index));
    }

    @Test
    public void shouldSetValuesInPageSizedChunks() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        // larger than the desired size, but still within a single page
        array.set(between(11, PS -1), 1337);
        // doesn't fail - good
    }

    // capacity check is only an assert
    @Test
    public void shouldUseCapacityInPageSizedChunks() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        // larger than the desired size, but still within a single page
        try {
            array.set(between(PS, 2 * PS - 1), 1337);
            fail("should have failed out of capacity");
        } catch (AssertionError | ArrayIndexOutOfBoundsException ignore) {
        }
    }

    @Test
    public void shouldHaveContains() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        int index = between(2, 8);
        int value = between(42, 1337);
        array.set(index, value);
        SparseNodeMapping longArray = array.build();
        for (int i = 0; i < 10; i++) {
            if (i == index) {
                assertTrue("Expected index " + i + " to be contained, but it was not", longArray.contains(i));
            } else {
                assertFalse("Expected index " + i + " not to be contained, but it actually was", longArray.contains(i));
            }
        }
    }

    @Test
    public void shouldReturnNegativeOneForMissingValues() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        int index = between(2, 8);
        int value = between(42, 1337);
        array.set(index, value);
        SparseNodeMapping longArray = array.build();
        for (int i = 0; i < 10; i++) {
            boolean expectedContains = i == index;
            long actual = longArray.get(i);
            String message = expectedContains
                    ? "Expected value at index " + i + " to be set to " + value + ", but got " + actual + " instead"
                    : "Expected value at index " + i + " to be missing (-1), but got " + actual + " instead";
            assertEquals(message, expectedContains ? value : -1L, actual);
        }
    }

    @Test
    public void shouldReturnNegativeOneForOutOfRangeValues() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(10, AllocationTracker.EMPTY);
        array.set(between(2, 8), between(42, 1337));
        assertEquals(-1L, array.build().get(between(100, 200)));
    }

    @Test
    public void shouldReturnNegativeOneForUnsetPages() {
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(PS + 10, AllocationTracker.EMPTY);
        array.set(5000, between(42, 1337));
        assertEquals(-1L, array.build().get(between(100, 200)));
    }

    @Test
    public void shouldNotCreateAnyPagesOnInitialization() throws Throwable {
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
    public void shouldCreateAndTrackSparsePagesOnDemand() throws Throwable {
        AllocationTracker tracker = AllocationTracker.create();
        SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(2 * PS, tracker);

        int index = between(PS, 2 * PS - 1);
        int value = between(42, 1337);
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

    @SuppressWarnings("unchecked")
    private static AtomicReferenceArray<long[]> getPages(SparseNodeMapping.Builder array) throws Throwable {
        return (AtomicReferenceArray<long[]>) PAGES.invoke(array);
    }
}
