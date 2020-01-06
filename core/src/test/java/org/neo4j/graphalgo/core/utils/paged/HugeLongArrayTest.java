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
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

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

    @Override
    HugeLongArray singleArray(final int size) {
        return HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY);
    }

    @Override
    HugeLongArray pagedArray(final int size) {
        return HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
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
