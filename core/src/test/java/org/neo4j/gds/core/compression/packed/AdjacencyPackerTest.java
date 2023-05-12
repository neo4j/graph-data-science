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
package org.neo4j.gds.core.compression.packed;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.params.IntRangeSource;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.SeededRandom.newRandom;
import static org.neo4j.gds.core.compression.common.CursorUtil.decompressCursor;

class AdjacencyPackerTest {

    @SuppressForbidden(reason = "Want to look at the data")
    @Test
    void roundtrip() {
        var random = newRandom();

        var values = random.random().longs(256, 0, 1L << 50).toArray();
        Arrays.sort(values);
        var originalValues = Arrays.copyOf(values, values.length);
        var uncompressedSize = originalValues.length * Long.BYTES;

        TestAllocator.testCursor(values, values.length, Aggregation.NONE, (cursor, slice) -> {

            var newRequiredBytes = slice.length();
            System.out.printf(
                Locale.ENGLISH,
                "packed = %d ratio = %.2f%n",
                newRequiredBytes,
                (double) newRequiredBytes / uncompressedSize
            );

            var decompressed = decompressCursor(cursor);
            assertThat(decompressed).containsExactly(originalValues);

            var varLongCompressed = AdjacencyCompression.deltaEncodeAndCompress(
                originalValues.clone(),
                0,
                originalValues.length,
                Aggregation.NONE
            );
            int requiredBytes = varLongCompressed.length;

            System.out.printf(
                Locale.ENGLISH,
                "var long = %d ratio = %.2f%n",
                requiredBytes,
                (double) requiredBytes / uncompressedSize
            );

            assertThat(newRequiredBytes)
                .as("new compressed should be less than dvl compressed, seed = %d", random.seed())
                .isLessThanOrEqualTo(requiredBytes);
        });

    }

    @Test
    void compressConsecutiveLongs() {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();

        TestAllocator.testCursor(data, data.length, Aggregation.NONE, (cursor, slice) -> {
            int maxBitsPerValues = 1; // delta + packing uses 1 bit for any value in this test
            int maxBitsPerBlock = maxBitsPerValues * AdjacencyPacking.BLOCK_SIZE;
            int maxBytesPerBlock = maxBitsPerBlock / Byte.SIZE;
            int maxBytes = 8 /* aligned header size */ + maxBytesPerBlock;

            assertThat(slice.length()).isLessThanOrEqualTo(maxBytes);
            assertThat(slice.slice().address()).isNotZero();

            long[] decompressed = decompressCursor(cursor);

            assertThat(decompressed).containsExactly(data);
        });
    }

    @Test
    void compressRandomLongs() {
        var random = newRandom();
        var data = random.random().longs(AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50).toArray();

        TestAllocator.testCursor(data, data.length, Aggregation.NONE, (cursor, slice) -> {
            assertThat(slice.length())
                .as("compressed exceeds original size, seed = %d", random.seed())
                .isLessThanOrEqualTo(1 + AdjacencyPacking.BLOCK_SIZE * Long.BYTES);
            assertThat(slice.slice().address()).isNotZero();

            Arrays.sort(data);
            var decompressed = decompressCursor(cursor);
            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(data);
        });
    }

    @Test
    void loopPack0Test() {
        long allocation = BitUtil.align(1, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);
        long next = AdjacencyPacking.loopPack(0, new long[0], 0, 0, ptr);
        assertThat(next).isEqualTo(ptr);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void loopPack5Test() {
        int bits = 5;
        long upperBound = (1L << bits) - 1;
        long[] data = LongStream.rangeClosed(0, upperBound).toArray();
        int length = data.length;
        int requiredBytes = BitUtil.ceilDiv(length * bits, Byte.SIZE);

        long allocation = BitUtil.align(requiredBytes, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);
        long next = AdjacencyPacking.loopPack(5, data, 0, length, ptr);

        var w0 = UnsafeUtil.getLong(ptr);
        assertThat(w0).isEqualTo(0b1100_01011_01010_01001_01000_00111_00110_00101_00100_00011_00010_00001_00000L);
        var w1 = UnsafeUtil.getLong(ptr + Long.BYTES);
        assertThat(w1).isEqualTo(0b001_11000_10111_10110_10101_10100_10011_10010_10001_10000_01111_01110_01101_0L);
        var w2 = UnsafeUtil.getLong(ptr + Long.BYTES + Long.BYTES);
        assertThat(w2).isEqualTo(0b11111_11110_11101_11100_11011_11010_11L);

        assertThat(next).isEqualTo(ptr + allocation);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void loopPack64Test() {
        int bits = 64;
        long upperBound = (1L << bits) - 1;
        long[] data = LongStream.concat(LongStream.rangeClosed(0, 42), LongStream.of(upperBound)).toArray();
        int length = data.length;
        int requiredBytes = BitUtil.ceilDiv(length * bits, Byte.SIZE);

        long allocation = BitUtil.align(requiredBytes, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);
        long next = AdjacencyPacking.loopPack(bits, data, 0, length, ptr);

        var w0 = UnsafeUtil.getLong(ptr);
        assertThat(w0).isEqualTo(0L);
        var w1 = UnsafeUtil.getLong(ptr + Long.BYTES);
        assertThat(w1).isEqualTo(1L);
        var w2 = UnsafeUtil.getLong(ptr + Long.BYTES + Long.BYTES);
        assertThat(w2).isEqualTo(2);
        var w42 = UnsafeUtil.getLong(ptr + Long.BYTES * 42);
        assertThat(w42).isEqualTo(42);
        var wUpperBound = UnsafeUtil.getLong(ptr + Long.BYTES * 43);
        assertThat(wUpperBound).isEqualTo(upperBound);

        assertThat(next).isEqualTo(ptr + allocation + Long.BYTES);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @ParameterizedTest
    @IntRangeSource(from = 0, to = 64, closed = true)
    void loopPackingRoundtripTest(int bits) {
        long upperBound = (1L << bits) - 1;
        long[] data = LongStream.concat(LongStream.range(0, Math.min(42, upperBound)), LongStream.of(upperBound))
            .toArray();
        int length = data.length;
        int requiredBytes = BitUtil.ceilDiv(length * bits, Byte.SIZE);

        long allocation = BitUtil.align(requiredBytes, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);

        AdjacencyPacking.loopPack(bits, data, 0, length, ptr);

        long[] unpacked = new long[(int) BitUtil.align(length, AdjacencyPacking.BLOCK_SIZE)];
        AdjacencyUnpacking.loopUnpack(bits, unpacked, 0, length, ptr);

        assertThat(Arrays.copyOf(unpacked, length)).isEqualTo(data);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void compressDuplicatedLongs() {
        var random = newRandom();
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE)
            .flatMap(value -> random.random().nextBoolean() ? LongStream.of(value) : LongStream.of(value, value))
            .limit(AdjacencyPacking.BLOCK_SIZE)
            .toArray();

        TestAllocator.testCursor(data, data.length, Aggregation.SINGLE, (cursor, slice) -> {
            int maxBitsPerValues = 6; // packed uses at most 6 bits for any value in this test
            int maxBitsPerBlock = maxBitsPerValues * AdjacencyPacking.BLOCK_SIZE;
            int maxBytesPerBlock = maxBitsPerBlock / Byte.SIZE;
            int maxBytes = 1 /* header size */ + maxBytesPerBlock;
            assertThat(slice.length())
                .as("compression uses more space than expected, seed = %s", random.seed())
                .isLessThanOrEqualTo(maxBytes);
            assertThat(slice.slice().address()).isNotZero();

            int degree = cursor.remaining();
            long[] expectedData = Arrays.stream(data).distinct().toArray();
            long[] decompressed = Arrays.copyOf(decompressCursor(cursor), degree);
            assertThat(decompressed).containsExactly(expectedData);
        });
    }

    @Test
    void compressRandomDuplicatedLongs() {
        var random = newRandom();
        var data = random.random().longs(2 * AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50)
            .distinct()
            .flatMap(value -> random.random().nextBoolean() ? LongStream.of(value) : LongStream.of(value, value))
            .limit(AdjacencyPacking.BLOCK_SIZE)
            .toArray();

        TestAllocator.testCursor(data, data.length, Aggregation.SINGLE, (cursor, slice) -> {

            assertThat(slice.length())
                .as("compressed exceeds original size, seed = %d", random.seed())
                .isLessThanOrEqualTo(1 + AdjacencyPacking.BLOCK_SIZE * Long.BYTES);
            assertThat(slice.slice().address()).isNotZero();

            var decompressed = decompressCursor(cursor);

            Arrays.sort(data);
            var expectedData = Arrays.stream(data).distinct().toArray();

            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(expectedData);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {42, 1337})
    void compressNonBlockAlignedConsecutiveLongs(int valueCount) {
        assertThat(valueCount % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var data = LongStream.range(0, valueCount).toArray();
        var alignedData = Arrays.copyOf(data, AdjacencyPacker.align(valueCount));

        TestAllocator.testCursor(alignedData, valueCount, Aggregation.NONE, (cursor, slice) -> {
            // TODO: we want to keep those assertions, but they fail with the current implementation
            // We have plans to improve this and eventually re-enable those assertions
            //        assertThat(compressed.bytesUsed())
            //            .as("compressed exceeds original size")
            //            .isLessThanOrEqualTo((long) valueCount * Long.BYTES);
            assertThat(slice.slice().address()).isNotZero();

            var decompressed = decompressCursor(cursor);

            assertThat(decompressed)
                .as("compressed data did not roundtrip")
                .containsExactly(data);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {42, 1337})
    void compressNonBlockAlignedRandomLongs(int valueCount) {
        assertThat(valueCount % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var random = newRandom();
        var data = random.random().longs(4242, 0, 1L << 50)
            .distinct()
            .limit(valueCount)
            .toArray();

        var alignedData = Arrays.copyOf(data, AdjacencyPacker.align(valueCount));

        TestAllocator.testCursor(alignedData, valueCount, Aggregation.NONE, (cursor, slice) -> {
            // TODO: we want to keep those assertions, but they fail with the current implementation
            // We have plans to improve this and eventually re-enable those assertions
//            assertThat(slice.length())
//                .as("compressed exceeds original size, seed = %d", random.seed())
//                .isLessThanOrEqualTo(valueCount * Long.BYTES);

            assertThat(slice.slice().address()).isNotZero();

            var decompressed = decompressCursor(cursor);

            Arrays.sort(data);
            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(data);
        });
    }
}
