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
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;

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
            assertThat(slice.slice()).isNotZero();

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
            assertThat(slice.slice()).isNotZero();

            Arrays.sort(data);
            var decompressed = decompressCursor(cursor);
            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(data);
        });
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
            assertThat(slice.slice()).isNotZero();

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
            assertThat(slice.slice()).isNotZero();

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
        var alignedData = Arrays.copyOf(data, AdjacencyPacker2.align(valueCount));

        TestAllocator.testCursor(alignedData, valueCount, Aggregation.NONE, (cursor, slice) -> {
            // TODO: we want to keep those assertions, but they fail with the current implementation
            // We have plans to improve this and eventually re-enable those assertions
            //        assertThat(compressed.bytesUsed())
            //            .as("compressed exceeds original size")
            //            .isLessThanOrEqualTo((long) valueCount * Long.BYTES);
            assertThat(slice.slice()).isNotZero();

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

        var alignedData = Arrays.copyOf(data, AdjacencyPacker2.align(valueCount));

        TestAllocator.testCursor(alignedData, valueCount, Aggregation.NONE, (cursor, slice) -> {
            // TODO: we want to keep those assertions, but they fail with the current implementation
            // We have plans to improve this and eventually re-enable those assertions
//            assertThat(slice.length())
//                .as("compressed exceeds original size, seed = %d", random.seed())
//                .isLessThanOrEqualTo(valueCount * Long.BYTES);

            assertThat(slice.slice()).isNotZero();

            var decompressed = decompressCursor(cursor);

            Arrays.sort(data);
            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(data);
        });
    }

    // TODO: Move to PackedAdjacencyListTest when this is implemented
//    @Test
//    void preventDoubleFree() {
//        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
//        var compressed = AdjacencyPacker.compress(data, 0, data.length);
//        assertThatCode(compressed::free).doesNotThrowAnyException();
//        assertThatThrownBy(compressed::free)
//            .isInstanceOf(IllegalStateException.class)
//            .hasMessage("This compressed memory has already been freed.");
//    }
//
//    @Test
//    void preventUseAfterFree() {
//        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
//        var compressed = AdjacencyPacker.compress(data, 0, data.length);
//        assertThatCode(compressed::free).doesNotThrowAnyException();
//        assertThatThrownBy(() -> AdjacencyPacker.decompress(compressed))
//            .isInstanceOf(IllegalStateException.class)
//            .hasMessage("This compressed memory has already been freed.");
//    }
}
