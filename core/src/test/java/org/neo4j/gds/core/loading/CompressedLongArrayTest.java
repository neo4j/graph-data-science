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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.core.loading.AdjacencyBuilder.IGNORE_VALUE;

class CompressedLongArrayTest {

    @Test
    void add() {
        CompressedLongArray compressedLongArray = new CompressedLongArray(AllocationTracker.empty());

        final long[] inValues = {1, 2, 3, 4};
        compressedLongArray.add(inValues.clone(), 0, inValues.length, inValues.length);

        assertTrue(compressedLongArray.storage().length >= inValues.length);

        long[] outValues = new long[4];
        int uncompressedValueCount = compressedLongArray.uncompress(outValues);

        assertEquals(4, uncompressedValueCount);
        assertArrayEquals(inValues, outValues);
    }

    @Test
    void addSameValues() {
        CompressedLongArray compressedLongArray1 = new CompressedLongArray(AllocationTracker.empty());
        CompressedLongArray compressedLongArray2 = new CompressedLongArray(AllocationTracker.empty());

        int count = 10;

        long[] inValues1 = LongStream.range(0, count).toArray();
        compressedLongArray1.add(inValues1, 0, count, count);

        long[] inValues2 = LongStream.range(0, count).toArray();
        for (int i = 0; i < count; i++) {
            compressedLongArray2.add(inValues2, i, i + 1, 1);
        }

        long[] outValues1 = new long[count];
        long[] outValues2 = new long[count];
        int uncompressedValueCount1 = compressedLongArray1.uncompress(outValues1);
        int uncompressedValueCount2 = compressedLongArray2.uncompress(outValues2);

        assertEquals(uncompressedValueCount1, uncompressedValueCount2);
        assertArrayEquals(outValues1, outValues2);
    }

    @Test
    void addReverseOrder() {
        CompressedLongArray compressedLongArray = new CompressedLongArray(AllocationTracker.empty());

        int count = 10;

        long[] inValues = LongStream.range(0, count).map(i -> count - i).toArray();
        compressedLongArray.add(inValues.clone(), 0, inValues.length, inValues.length);

        assertTrue(compressedLongArray.storage().length >= 10);

        long[] outValues = new long[count];
        int uncompressedValueCount = compressedLongArray.uncompress(outValues);

        assertEquals(count, uncompressedValueCount);
        assertArrayEquals(inValues, outValues);
    }

    @Test
    void addWithPreAggregation() {
        var valuesToAdd = 3;
        CompressedLongArray compressedLongArray = new CompressedLongArray(AllocationTracker.empty());

        final long[] inValues = {1, IGNORE_VALUE, IGNORE_VALUE, 2, 3};
        compressedLongArray.add(inValues.clone(), 0, inValues.length, valuesToAdd);

        assertTrue(compressedLongArray.storage().length >= valuesToAdd);

        long[] outValues = new long[3];
        int uncompressedValueCount = compressedLongArray.uncompress(outValues);

        assertEquals(3, uncompressedValueCount);
        assertArrayEquals(new long[]{1, 2, 3}, outValues);
    }

    @Test
    void addWithWeights() {
        CompressedLongArray compressedLongArray = new CompressedLongArray(AllocationTracker.empty(), 1);

        final long[] inValues = {1, 2, 3, 4};
        final long[] inWeights = DoubleStream.of(1.0, 2.0, 3.0, 4.0).mapToLong(Double::doubleToLongBits).toArray();
        compressedLongArray.add(inValues.clone(), new long[][]{inWeights.clone()}, 0, inValues.length, inValues.length);

        // 10 bytes are enough to store the input values (1 byte each)
        assertTrue(compressedLongArray.storage().length >= inValues.length);

        long[] outValues = new long[4];
        int uncompressedValueCount = compressedLongArray.uncompress(outValues);
        assertEquals(4, uncompressedValueCount);
        assertArrayEquals(inValues, outValues);

        assertArrayEquals(inWeights, Arrays.copyOf(compressedLongArray.weights()[0], inWeights.length));
    }

    @Test
    void addWithPreAggregatedWeights() {
        CompressedLongArray compressedLongArray = new CompressedLongArray(AllocationTracker.empty(), 1);

        var inValues = new long[]{1, IGNORE_VALUE, 2, 3};
        var expectedValues = new long[]{1, 2, 3};
        var inWeights = DoubleStream.of(3.0, 2.0, 3.0, 4.0).mapToLong(Double::doubleToLongBits).toArray();
        var expectedWeights = DoubleStream.of(3.0, 3.0, 4.0).mapToLong(Double::doubleToLongBits).toArray();

        compressedLongArray.add(inValues.clone(), new long[][]{inWeights.clone()}, 0, inValues.length, 3);

        // 10 bytes are enough to store the input values (1 byte each)
        assertTrue(compressedLongArray.storage().length >= inValues.length);

        long[] outValues = new long[3];
        int uncompressedValueCount = compressedLongArray.uncompress(outValues);
        assertEquals(3, uncompressedValueCount);

        assertArrayEquals(expectedValues, outValues);

        assertArrayEquals(expectedWeights, Arrays.copyOf(compressedLongArray.weights()[0], expectedWeights.length));
    }

    @Test
    void throwsOnOverflowInEnsureCapacity() {
        CompressedLongArray longArray = new CompressedLongArray(AllocationTracker.empty());
        var e = assertThrows(
            IllegalArgumentException.class,
            () -> {
                int almostIntMax = 2147483622;
                givenAnByteArrayOfUnsafeFakeLength(almostIntMax, data -> {
                    longArray.ensureCapacity(almostIntMax, 100, data);
                });
            }
        );
        assertTrue(e.getMessage().contains("numeric overflow in internal buffer"));
    }

    private void givenAnByteArrayOfUnsafeFakeLength(int length, Consumer<byte[]> code) {
        /*
        byte[] B = new byte[14];
        [B object internals:
         OFFSET  SIZE   TYPE DESCRIPTION                               VALUE
              0     4        (object header)                           01 00 00 00 (00000001 00000000 00000000 00000000) (1)
              4     4        (object header)                           00 00 00 00 (00000000 00000000 00000000 00000000) (0)
              8     4        (object header)                           20 08 00 00 (00100000 00001000 00000000 00000000) (2080)
             12     4        (object header)                           0e 00 00 00 (00001110 00000000 00000000 00000000) (14)
             16    14   byte [B.<elements>                             N/A
             30     2        (loss due to the next object alignment)
        Instance size: 32 bytes
        Space losses: 0 bytes internal + 2 bytes external = 2 bytes total
         */
        var unsafe = findTheUnsafe();
        // Offset 16 where the array elements start
        int arrayBaseOffset = unsafe.arrayBaseOffset(byte[].class);
        // Offset 12 which is the `length` field in the array object header
        int arrayLengthOffset = arrayBaseOffset - Integer.BYTES;

        byte[] bytes = {};
        // Override the internal `length` field to be the requested `length`
        unsafe.putIntVolatile(bytes, arrayLengthOffset, length);
        assertEquals(length, bytes.length);

        try {
            code.accept(bytes);
        } finally {
            // Set it back to 0 to avoid shenanigans when it is cleaned up
            unsafe.putIntVolatile(bytes, arrayLengthOffset, 0);
        }
    }

    private static sun.misc.Unsafe findTheUnsafe() {
        org.neo4j.internal.unsafe.UnsafeUtil.assertHasUnsafe();
        try {
            var unsafeField = org.neo4j.internal.unsafe.UnsafeUtil.class.getDeclaredField("unsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new LinkageError("Could not find the unsafe", e);
        }
    }
}
