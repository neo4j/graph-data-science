/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.ObjIntConsumer;

import static io.qala.datagen.RandomShortApi.bool;
import static io.qala.datagen.RandomShortApi.integer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class HugeArrayTestBase<Array, Box, Huge extends HugeArray<Array, Box, Huge>> {

    private static final int PS = HugeArrays.PAGE_SIZE;

    @Test
    final void shouldSetAndGet() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);

            array.boxedSet(index, box(value));
            assertEquals(value, get(array, index));
        });
    }

    @Test
    final void shouldSetAllValues() {
        int size = integer(10, 20);
        testArray(size, array -> {
            array.boxedSetAll(i -> box(1 << i));
            for (int index = 0; index < size; index++) {
                assertEquals(1L << index, get(array, index));
            }
        });
    }

    @Test
    final void shouldFillValues() {
        int size = integer(10, 20);
        int value = integer(42, 1337);
        testArray(size, array -> {
            array.boxedFill(box(value));
            for (int index = 0; index < size; index++) {
                assertEquals(value, get(array, index));
            }
        });
    }

    @Test
    final void shouldCopyValues() {
        int size = integer(10, 20);
        testArray(size, array -> {
            testArray(size, target -> {
                array.boxedSetAll(i1 -> box((int) i1 + 1));
                array.copyTo(target, size);
                for (int i = 0; i < size; i++) {
                    assertEquals(i + 1, get(target, i));
                }
            });
        });
    }

    @Test
    final void shouldResetValuesNotCopied() {
        testArray(10, 20, (array, size) -> {
            testArray(30, 40, (target, sizeTarget) -> {
                array.boxedFill(box(42));
                target.boxedFill(box(1337));

                array.copyTo(target, size);

                for (int i = 0; i < size; i++) {
                    assertEquals(42L, get(target, i));
                }
                for (int i = size; i < sizeTarget; i++) {
                    assertEquals(0L, get(target, i));
                }
            });
        });
    }

    @Test
    final void shouldOnlyCopyValuesThatAreADefined() {
        testArray(30, 40, (array, size) -> {
            testArray(10, 20, (target, sizeTarget) -> {
                array.boxedFill(box(42));
                array.copyTo(target, size);

                for (int i = 0; i < sizeTarget; i++) {
                    assertEquals(42L, get(target, i));
                }
            });
        });
    }

    @Test
    final void shouldArrayCopy() {
        testArray(13, 42, (source, sizeSource) -> {
            testArray(13, 42, (target, sizeTarget) -> {
                testArrayCopy(source, sizeSource, target, sizeTarget);
            });
        });
    }

    @Test
    final void shouldArrayLargePages() {
        int sizeSource = integer(100_000, 200_000);
        int sizeTarget = integer(100_000, 200_000);
        Huge source = pagedArray(sizeSource);
        Huge target = pagedArray(sizeTarget);

        testArrayCopy(source, sizeSource, target, sizeTarget);
    }

    private void testArrayCopy(
            Huge source,
            int sizeSource,
            Huge target,
            int sizeTarget) {
        int copySize = Math.max(sizeSource, sizeTarget);
        int expectedCopySize = Math.min(sizeTarget, Math.min(copySize, sizeSource));

        source.boxedFill(box(42));
        target.boxedFill(box(1337));

        source.copyTo(target, copySize);

        for (int i = 0; i < expectedCopySize; i++) {
            assertEquals(42L, get(target, i));
        }
        for (int i = expectedCopySize; i < sizeTarget; i++) {
            assertEquals(0L, get(target, i));
        }
    }

    @Test
    final void shouldReportSize() {
        int size = integer(10, 20);
        testArray(size, array -> {
            assertEquals(size, array.size());
        });
    }

    @Test
    final void shouldFreeMemoryUsed() {
        int size = integer(10, 20);
        long expected = bufferSize(size);
        testArray(size, array -> {
            long freed = array.release();
            assertThat(freed, anyOf(is(expected), is(expected + 24)));
        });
    }

    @Test
    final void shouldHaveSinglePageCursor() {
        int size = integer(100, 200);
        Huge array = singleArray(size);
        array.boxedFill(box(42));
        HugeCursor<Array> cursor = array.initCursor(array.newCursor());

        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(size, cursor.limit);

        testCursorContent(size, cursor);

        assertFalse(cursor.next());
    }

    @Test
    final void shouldHaveCursorForSinglePage() {
        int size = integer(100, 200);
        Huge array = pagedArray(size);
        array.boxedFill(box(42));
        HugeCursor<Array> cursor = array.initCursor(array.newCursor());

        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(size, cursor.limit);

        testCursorContent(size, cursor);

        assertFalse(cursor.next());
    }

    private void testCursorContent(int size, HugeCursor<Array> cursor) {
        Box[] expected = newBoxedArray(size);
        Arrays.fill(expected, box(42));
        compareAgainst(cursor.array, cursor.offset, cursor.limit - cursor.offset, expected);
    }

    @Test
    final void shouldHaveCursorForMultiplePages() {
        int size = integer(100_000, 200_000);
        Huge array = pagedArray(size);
        array.boxedFill(box(42));
        HugeCursor<Array> cursor = array.initCursor(array.newCursor());

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
    final void shouldHavePartialCursorForMultiplePages() {
        int size = integer(100_000, 200_000);
        int start = integer(10_000, 50_000);
        int end = integer(start + 2 * PS, size);
        testPartialMultiCursor(size, start, end);
    }

    @Test
    final void shouldHavePartialCursorForMultiplePagesWithFullPageSized() {
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

    @SuppressWarnings("unchecked")
    private void testPartialMultiCursor(int size, int start, int end) {
        Huge array = pagedArray(size);
        array.boxedSetAll(i1 -> box(42 + (int) i1));
        HugeCursor<Array> cursor = array.newCursor();
        array.initCursor(cursor, start, end);

        long expected = start + 42L;
        while (cursor.next()) {
            for (int i = cursor.offset; i < cursor.limit; i++, expected++) {
                Box value = (Box) java.lang.reflect.Array.get(cursor.array, i);
                assertEquals(expected, unbox(value));
            }
        }
        assertEquals(expected, end + 42L);
    }

    @SuppressWarnings("unchecked")
    @Test
    final void shouldHaveCursor() {
        int size = integer(100_000, 200_000);
        testArray(size, array -> {
            array.boxedSetAll(i1 -> box((int) i1 + 1));

            long actual = 0L;
            HugeCursor<Array> cursor = array.initCursor(array.newCursor());
            while (cursor.next()) {
                int offset = cursor.offset;
                int limit = cursor.limit;
                for (int i = offset; i < limit; i++) {
                    Box value = (Box) java.lang.reflect.Array.get(cursor.array, i);
                    actual += unbox(value);
                }
            }

            long sum = ((long) size * (long) (size + 1)) / 2L;
            assertEquals(actual, sum);
        });
    }

    @Test
    final void shouldHaveStringRepresentation() {
        testArray(10, 20, (array, size) -> {
            Object[] objects = new Object[size];
            Arrays.setAll(objects, i -> box(i + 2));
            String expected = Arrays.toString(objects);

            array.boxedSetAll(i -> box((int) i + 2));
            String actual = array.toString();
            assertEquals(expected, actual);
        });
    }

    @Test
    final void shouldHaveToArray() {
        testArray(10, 20, (array, size) -> {
            array.boxedSetAll(i -> box((int) i + 2));

            Box[] expected = newBoxedArray(size);
            Arrays.setAll(expected, i -> box(i + 2));
            compareAgainst(array.toArray(), expected);
        });
    }

    @Test
    final void shouldCopyFromArrayOfSameSize() {
        testArray(10, 20, (array, size) -> {
            Array source = newUnderlyingArray(size);
            setAll(source, i -> box(i + 2));

            array.copyFromArrayIntoSlice(source, 0, size);

            Box[] expected = newBoxedArray(size);
            Arrays.setAll(expected, i -> box(i + 2));
            compareAgainst(array.toArray(), expected);
        });
    }

    @Test
    final void shouldCopyFromSmallerArray() {
        testArray(10, 20, (array, size) -> {
            int sourceSize = size - 5;
            Array source = newUnderlyingArray(sourceSize);
            setAll(source, i -> box(i + 2));

            array.copyFromArrayIntoSlice(source, 0, size);

            // make sure that the items that weren't copied from are 0
            // and not the default null that a boxed array would return
            Box[] expected = newBoxedArray(size);
            Arrays.setAll(expected, i -> i < sourceSize ? box(i + 2) : primitiveNull());
            compareAgainst(array.toArray(), expected);
        });
    }

    @Test
    final void shouldCopyFromLargerArray() {
        testArray(10, 20, (array, size) -> {
            int sourceSize = size + 5;
            Array source = newUnderlyingArray(sourceSize);
            setAll(source, i -> box(i + 2));

            array.copyFromArrayIntoSlice(source, 0, size);

            Box[] expected = newBoxedArray(size);
            Arrays.setAll(expected, i -> box(i + 2));
            compareAgainst(array.toArray(), expected);
        });
    }

    @Test
    final void shouldCopyIntoSmallerSlice() {
        testArray(10, 20, (array, size) -> {
            Array source = newUnderlyingArray(size);
            setAll(source, i -> box(i + 2));

            int targetSize = size - 5;
            array.copyFromArrayIntoSlice(source, 0, targetSize);

            // make sure that the items that weren't copied from are 0
            // and not the default null that a boxed array would return
            Box[] expected = newBoxedArray(size);
            Arrays.setAll(expected, i -> i < targetSize ? box(i + 2) : primitiveNull());
            compareAgainst(array.toArray(), expected);
        });
    }

    @Test
    final void shouldNotCopyIntoLargerSlice() {
        testArray(10, 20, (array, size) -> {
            int targetSize = size + 5;
            AssertionError assertionError = assertThrows(AssertionError.class, () -> {
                Array source = newUnderlyingArray(size);
                array.copyFromArrayIntoSlice(source, 0, targetSize);
            });
            assertEquals(
                    String.format("end expected to be in [0 : %d] but got %d", size, targetSize),
                    assertionError.getMessage()
            );
        });
    }

    void testArray(int size, Consumer<Huge> block) {
        if (bool()) {
            block.accept(singleArray(size));
            block.accept(pagedArray(size));
        } else {
            block.accept(pagedArray(size));
            block.accept(singleArray(size));
        }
    }

    private void testArray(int sizeMin, int sizeMax, ObjIntConsumer<Huge> block) {
        int size;
        if (bool()) {
            size = integer(sizeMin, sizeMax);
            block.accept(singleArray(size), size);
            size = integer(sizeMin, sizeMax);
            block.accept(pagedArray(size), size);
        } else {
            size = integer(sizeMin, sizeMax);
            block.accept(pagedArray(size), size);
            size = integer(sizeMin, sizeMax);
            block.accept(singleArray(size), size);
        }
    }

    abstract Huge singleArray(int size);

    abstract Huge pagedArray(int size);

    abstract long bufferSize(int size);

    abstract Box box(int value);

    abstract int unbox(Box value);

    abstract Box primitiveNull();

    private void compareAgainst(Array array, Box[] expected) {
        compareAgainst(array, 0, java.lang.reflect.Array.getLength(array), expected);
    }

    private void compareAgainst(Array array, int offset, int length, Box[] expected) {
        if (array instanceof int[]) {
            int[] e = Arrays.stream(expected).mapToInt(this::unbox).toArray();
            int[] actual = Arrays.copyOfRange((int[]) array, offset, length - offset);
            assertArrayEquals(e, actual);
        }

        else if (array instanceof long[]) {
            long[] e = Arrays.stream(expected).mapToLong(this::unbox).toArray();
            long[] actual = Arrays.copyOfRange((long[]) array, offset, length - offset);
            assertArrayEquals(e, actual);
        }

        else if (array instanceof double[]) {
            double[] e = Arrays.stream(expected).mapToDouble(this::unbox).toArray();
            double[] actual = Arrays.copyOfRange((double[]) array, offset, length - offset);
            assertArrayEquals(e, actual, 1e-4);
        }

        else if (array instanceof String[]) {
            String[] e = Arrays.stream(expected).map(s -> Objects.toString(s, null)).toArray(String[]::new);
            String[] actual = Arrays.copyOfRange((String[]) array, offset, length - offset);
            assertArrayEquals(e, actual);
        }
    }

    private int get(Huge array, int index) {
        return unbox(array.boxedGet(index));
    }

    @SuppressWarnings("unchecked")
    private Box[] newBoxedArray(final int size) {
        return (Box[]) java.lang.reflect.Array.newInstance(box(42).getClass(), size);
    }

    @SuppressWarnings("unchecked")
    private Array newUnderlyingArray(final int size) {
        Class<?> arrayType = box(42).getClass();
        if (arrayType == Integer.class) {
            arrayType = Integer.TYPE;
        } else if (arrayType == Long.class) {
            arrayType = Long.TYPE;
        } else if (arrayType == Double.class) {
            arrayType = Double.TYPE;
        }
        return (Array) java.lang.reflect.Array.newInstance(arrayType, size);
    }

    private void setAll(Array array, IntFunction<Box> gen) {
        int size = java.lang.reflect.Array.getLength(array);
        for (int i = 0; i < size; i++) {
            Box value = gen.apply(i);
            if (value instanceof Integer) {
                java.lang.reflect.Array.setInt(array, i, (Integer) value);
            } else if (value instanceof Long) {
                java.lang.reflect.Array.setLong(array, i, (Long) value);
            } else if (value instanceof Double) {
                java.lang.reflect.Array.setDouble(array, i, (Double) value);
            } else {
                java.lang.reflect.Array.set(array, i, value);
            }
        }
    }
}
