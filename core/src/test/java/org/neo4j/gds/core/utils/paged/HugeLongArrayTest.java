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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.mem.HugeArrays;
import org.neo4j.gds.mem.MemoryUsage;

import static io.qala.datagen.RandomShortApi.integer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HugeLongArrayTest extends HugeArrayTestBase<long[], Long, HugeLongArray> {

    @Test
    void shouldBinaryOrValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);
            array.set(index, value);
            int newValue = integer(42, 1337);
            array.or(index, newValue);
            assertEquals(value | newValue, array.get(index));
        });
    }

    @Test
    void shouldBinaryAndValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);
            array.set(index, value);
            int newValue = integer(42, 1337);
            array.and(index, newValue);
            assertEquals(value & newValue, array.get(index));
        });
    }

    @Test
    void shouldAddToValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);
            array.set(index, value);
            int newValue = integer(42, 1337);
            array.addTo(index, newValue);
            assertEquals(value + newValue, array.get(index));
        });
    }

    @Test
    void shouldComputeMemoryEstimation() {
        assertEquals(40, HugeLongArray.memoryEstimation(0L));
        assertEquals(840, HugeLongArray.memoryEstimation(100L));
        assertEquals(800_122_070_368L, HugeLongArray.memoryEstimation(100_000_000_000L));
    }

    @Test
    void shouldFailForNegativeMemRecSize() {
        assertThrows(AssertionError.class, () -> HugeLongArray.memoryEstimation(-1L));
    }

    @Test
    void shouldBinarySearchInASinglePageArray() {
        var array = HugeLongArray.newSingleArray(
            10
        );
        for (int i = 0; i < 10; i++) {
            array.set(i, i);
        }

        assertEquals(5, array.binarySearch(5));
        assertEquals(9, array.binarySearch(20));
        assertEquals(-1, array.binarySearch(-10));
    }

    @Test
    void shouldBinarySearchInAPagedArray() {
        var array = HugeLongArray.newPagedArray(
            HugeArrays.PAGE_SIZE * 3
        );
        for (int i = 0; i < HugeArrays.PAGE_SIZE * 3; i++) {
            array.set(i, i);
        }

        assertEquals(20000, array.binarySearch(20000));
        assertEquals(HugeArrays.PAGE_SIZE * 3 - 1, array.binarySearch(HugeArrays.PAGE_SIZE * 3 + 10));
        assertEquals(-1, array.binarySearch(-10));
    }

    @Override
    HugeLongArray singleArray(final int size) {
        return HugeLongArray.newSingleArray(size);
    }

    @Override
    HugeLongArray pagedArray(final int size) {
        return HugeLongArray.newPagedArray(size);
    }

    @Override
    long bufferSize(final int size) {
        return MemoryUsage.sizeOfLongArray(size);
    }

    @Override
    Long box(final int value) {
        return (long) value;
    }

    @Override
    int unbox(final Long value) {
        return value.intValue();
    }

    @Override
    Long primitiveNull() {
        return 0L;
    }
}
