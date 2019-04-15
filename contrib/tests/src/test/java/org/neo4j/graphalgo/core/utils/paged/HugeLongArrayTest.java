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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public final class HugeLongArrayTest extends RandomizedTest {

    private static final int PS = 16384;

    @Test
    public void shouldSetAndGet() {
        testArray(10, array -> {
            int index = between(2, 8);
            int value = between(42, 1337);
            array.set(index, value);
            assertEquals(value, array.get(index));
        });
    }

    @Test
    public void shouldBinaryOrValues() {
        testArray(10, array -> {
            int index = between(2, 8);
            int value = between(42, 1337);
            array.set(index, value);
            int newValue = between(42, 1337);
            array.or(index, newValue);
            assertEquals(value | newValue, array.get(index));
        });
    }

    @Test
    public void shouldBinaryAndValues() {
        testArray(10, array -> {
            int index = between(2, 8);
            int value = between(42, 1337);
            array.set(index, value);
            int newValue = between(42, 1337);
            array.and(index, newValue);
            assertEquals(value & newValue, array.get(index));
        });
    }

    @Test
    public void shouldAddToValues() {
        testArray(10, array -> {
            int index = between(2, 8);
            int value = between(42, 1337);
            array.set(index, value);
            int newValue = between(42, 1337);
            array.addTo(index, newValue);
            assertEquals(value + newValue, array.get(index));
        });
    }

    @Test
    public void shouldSetAllValues() {
        int size = between(10, 20);
        testArray(size, array -> {
            array.setAll(index -> 1L << index);
            for (int index = 0; index < size; index++) {
                assertEquals(1L << index, array.get(index));
            }
        });
    }

    @Test
    public void shouldFillValues() {
        int size = between(10, 20);
        int value = between(42, 1337);
        testArray(size, array -> {
            array.fill(value);
            for (int index = 0; index < size; index++) {
                assertEquals(value, array.get(index));
            }
        });
    }

    @Test
    public void shouldCopyValues() {
        int size = between(10, 20);
        testArray(size, array -> {
            testArray(size, target -> {
                array.setAll(i -> i + 1);

                array.copyTo(target, size);

                for (int i = 0; i < size; i++) {
                    assertEquals(i + 1, target.get(i));
                }
            });
        });
    }

    @Test
    public void shouldResetValuesNotCopied() {
        testArray(10, 20, (array, size) -> {
            testArray(30, 40, (target, sizeTarget) -> {
                array.fill(42L);
                target.fill(1337L);

                array.copyTo(target, size);

                for (int i = 0; i < size; i++) {
                    assertEquals(42L, target.get(i));
                }
                for (int i = size; i < sizeTarget; i++) {
                    assertEquals(0L, target.get(i));
                }
            });
        });
    }

    @Test
    public void shouldOnlyCopyValuesThatAreADefined() {
        testArray(30, 40, (array, size) -> {
            testArray(10, 20, (target, sizeTarget) -> {
                array.fill(42L);

                array.copyTo(target, size);

                for (int i = 0; i < sizeTarget; i++) {
                    assertEquals(42L, target.get(i));
                }
            });
        });
    }

    @Test
    public void shouldArrayCopy() {
        testArray(13, 42, (source, sizeSource) -> {
            testArray(13, 42, (target, sizeTarget) -> {
                testArrayCopy(source, sizeSource, target, sizeTarget);
            });
        });
    }

    @Test
    public void shouldArrayLargePages() {
        int sizeSource = between(100_000, 200_000);
        int sizeTarget = between(100_000, 200_000);
        HugeLongArray source = HugeLongArray.newPagedArray(sizeSource, AllocationTracker.EMPTY);
        HugeLongArray target = HugeLongArray.newPagedArray(sizeTarget, AllocationTracker.EMPTY);

        testArrayCopy(source, sizeSource, target, sizeTarget);
    }

    private void testArrayCopy(
            final HugeLongArray source,
            final int sizeSource,
            final HugeLongArray target,
            final int sizeTarget) {
        int copySize = Math.max(sizeSource, sizeTarget);
        int expectedCopySize = Math.min(sizeTarget, Math.min(copySize, sizeSource));

        source.fill(42L);
        target.fill(1337L);

        source.copyTo(target, copySize);

        for (int i = 0; i < expectedCopySize; i++) {
            assertEquals(42L, target.get(i));
        }
        for (int i = expectedCopySize; i < sizeTarget; i++) {
            assertEquals(0L, target.get(i));
        }
    }

    @Test
    public void shouldReportSize() {
        int size = between(10, 20);
        testArray(size, array -> {
            assertEquals(size, array.size());
        });
    }

    @Test
    public void shouldFreeMemoryUsed() {
        int size = between(10, 20);
        final long expected = MemoryUsage.sizeOfLongArray(size);
        testArray(size, array -> {
            final long freed = array.release();
            assertThat(freed, anyOf(is(expected), is(expected + 24)));
        });
    }

    @Test
    public void shouldHaveSinglePageCursor() {
        int size = between(100, 200);
        HugeLongArray array = HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY);
        array.fill(42L);
        HugeLongArray.Cursor cursor = array.cursor(array.newCursor());

        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(size, cursor.limit);

        final long[] expected = new long[size];
        Arrays.fill(expected, 42L);
        final long[] actual = Arrays.copyOfRange(cursor.array, cursor.offset, cursor.limit);
        assertArrayEquals(expected, actual);

        assertFalse(cursor.next());
    }

    @Test
    public void shouldHaveCursorForSinglePage() {
        int size = between(100, 200);
        HugeLongArray array = HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
        array.fill(42L);
        HugeLongArray.Cursor cursor = array.cursor(array.newCursor());

        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(size, cursor.limit);

        final long[] expected = new long[size];
        Arrays.fill(expected, 42L);
        final long[] actual = Arrays.copyOfRange(cursor.array, cursor.offset, cursor.limit);
        assertArrayEquals(expected, actual);

        assertFalse(cursor.next());
    }

    @Test
    public void shouldHaveCursorForMultiplePages() {
        int size = between(100_000, 200_000);
        HugeLongArray array = HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
        array.fill(42L);
        HugeLongArray.Cursor cursor = array.cursor(array.newCursor());

        // first page
        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(PS, cursor.limit);

        // middle pages
        int pageEnd = PS;
        while (pageEnd + PS < size) {
            assertTrue(cursor.next());
            assertEquals(0, cursor.offset);
            assertEquals(PS, cursor.limit);
            assertEquals(pageEnd, cursor.base);
            pageEnd += PS;
        }

        // last page
        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(pageEnd, cursor.base);
        assertEquals(size & (PS - 1), cursor.limit);

        assertFalse(cursor.next());
    }

    @Test
    public void shouldHavePartialCursorForMultiplePages() {
        int size = between(100_000, 200_000);
        int start = between(10_000, 50_000);
        int end = between(start + 2 * PS, size);
        testPartialMultiCursor(size, start, end);
    }

    @Test
    public void shouldHavePartialCursorForMultiplePagesWithFullPageSized() {
        testPartialMultiCursor(PS, 0, 0);
        testPartialMultiCursor(PS, 0, PS);
        testPartialMultiCursor(PS, PS, PS);

        testPartialMultiCursor(2 * PS, 0, 0);
        testPartialMultiCursor(2 * PS, 0, PS);
        testPartialMultiCursor(2 * PS, 0, 2 * PS);
        testPartialMultiCursor(2 * PS, PS, PS);
        testPartialMultiCursor(2 * PS, PS, 2 * PS);
        testPartialMultiCursor(2 * PS, 2 * PS, 2 * PS);

        testPartialMultiCursor(3 * PS, 0, 0);
        testPartialMultiCursor(3 * PS, 0, PS);
        testPartialMultiCursor(3 * PS, 0, 2 * PS);
        testPartialMultiCursor(3 * PS, 0, 3 * PS);
        testPartialMultiCursor(3 * PS, PS, PS);
        testPartialMultiCursor(3 * PS, PS, 2 * PS);
        testPartialMultiCursor(3 * PS, PS, 3 * PS);
        testPartialMultiCursor(3 * PS, 2 * PS, 2 * PS);
        testPartialMultiCursor(3 * PS, 2 * PS, 3 * PS);
        testPartialMultiCursor(3 * PS, 3 * PS, 3 * PS);
    }

    private void testPartialMultiCursor(int size, int start, int end) {
        HugeLongArray array = HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
        array.setAll(i -> 42L + i);
        HugeLongArray.Cursor cursor = array.newCursor();
        array.cursor(cursor, start, end);

        long expected = start + 42L;
        while (cursor.next()) {
            for (int i = cursor.offset; i < cursor.limit; i++, expected++) {
                assertEquals(expected, cursor.array[i]);
            }
        }
        assertEquals(expected, end + 42L);
    }

    @Test
    public void shouldHaveCursor() {
        int size = between(100_000, 200_000);
        testArray(size, array -> {
            array.setAll(i -> i + 1L);

            long actual = 0L;
            final HugeLongArray.Cursor cursor = array.cursor(array.newCursor());
            while (cursor.next()) {
                long[] ar = cursor.array;
                int offset = cursor.offset;
                int limit = cursor.limit;
                for (int i = offset; i < limit; i++) {
                    actual += ar[i];
                }
            }

            final long sum = ((long) size * (long) (size + 1)) / 2L;
            assertEquals(actual, sum);
        });
    }

    private void testArray(int size, Consumer<HugeLongArray> block) {
        if (randomBoolean()) {
            block.accept(HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY));
            block.accept(HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY));
        } else {
            block.accept(HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY));
            block.accept(HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY));
        }
    }

    private void testArray(int sizeMin, int sizeMax, ObjIntConsumer<HugeLongArray> block) {
        int size;
        if (randomBoolean()) {
            size = between(sizeMin, sizeMax);
            block.accept(HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY), size);
            size = between(sizeMin, sizeMax);
            block.accept(HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY), size);
        } else {
            size = between(sizeMin, sizeMax);
            block.accept(HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY), size);
            size = between(sizeMin, sizeMax);
            block.accept(HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY), size);
        }
    }
}
