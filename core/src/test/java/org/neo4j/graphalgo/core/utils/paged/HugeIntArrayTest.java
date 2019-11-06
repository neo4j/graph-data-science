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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import static io.qala.datagen.RandomShortApi.integer;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class HugeIntArrayTest extends HugeArrayTestBase<int[], Integer, HugeIntArray> {

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

    @Override
    HugeIntArray singleArray(final int size) {
        return HugeIntArray.newSingleArray(size, AllocationTracker.EMPTY);
    }

    @Override
    HugeIntArray pagedArray(final int size) {
        return HugeIntArray.newPagedArray(size, AllocationTracker.EMPTY);
    }

    @Override
    long bufferSize(final int size) {
        return MemoryUsage.sizeOfIntArray(size);
    }

    @Override
    Integer box(final int value) {
        return value;
    }

    @Override
    int unbox(final Integer value) {
        return value;
    }

    @Override
    Integer primitiveNull() {
        return 0;
    }
}
