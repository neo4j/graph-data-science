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
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.mem.MemoryUsage;

import static io.qala.datagen.RandomShortApi.integer;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class HugeByteArrayTest extends HugeArrayTestBase<byte[], Byte, HugeByteArray> {

    @Test
    void shouldBinaryOrValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            byte value = (byte) integer(0, Byte.MAX_VALUE);
            array.set(index, value);
            byte newValue = (byte) integer(0, Byte.MAX_VALUE);
            array.or(index, newValue);
            assertEquals((byte) (value | newValue), array.get(index));
        });
    }

    @Test
    void shouldBinaryAndValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            byte value = (byte) integer(0, Byte.MAX_VALUE);
            array.set(index, value);
            byte newValue = (byte) integer(0, Byte.MAX_VALUE);
            array.and(index, newValue);
            assertEquals((byte) (value & newValue), array.get(index));
        });
    }

    @Test
    void shouldGetAndAddValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            byte value = (byte) integer(0, Byte.MAX_VALUE);
            byte delta = (byte) integer(0, Byte.MAX_VALUE);
            array.set(index, value);
            var oldValue = array.getAndAdd(index, delta);
            assertEquals(oldValue, value);
            assertEquals((byte) (value + delta), array.get(index));
        });
    }

    @Test
    void shouldAddToValues() {
        testArray(10, array -> {
            int index = integer(2, 8);
            byte value = (byte) integer(0, Byte.MAX_VALUE);
            array.set(index, value);
            byte newValue = (byte) integer(0, Byte.MAX_VALUE);
            array.addTo(index, newValue);
            assertEquals((byte) (value + newValue), array.get(index));
        });
    }

    @Test
    void shouldComputeMemoryEstimation() {
        assertEquals(40, HugeByteArray.memoryEstimation(0L));
        assertEquals(144, HugeByteArray.memoryEstimation(100L));
        assertEquals(100_122_070_368L, HugeByteArray.memoryEstimation(100_000_000_000L));
    }

    @Override
    HugeByteArray singleArray(final int size) {
        return HugeByteArray.newSingleArray(size, AllocationTracker.empty());
    }

    @Override
    HugeByteArray pagedArray(final int size) {
        return HugeByteArray.newPagedArray(size, AllocationTracker.empty());
    }

    @Override
    long bufferSize(final int size) {
        return MemoryUsage.sizeOfByteArray(size);
    }

    @Override
    Byte box(final int value) {
        return (byte) value;
    }

    @Override
    int unbox(final Byte value) {
        return value;
    }

    @Override
    Byte primitiveNull() {
        return 0;
    }

}
