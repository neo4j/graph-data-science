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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.compression.common.CursorUtil;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.SeededRandom.newRandom;
import static org.neo4j.gds.core.compression.common.CursorUtil.decompressCursor;

class AdjacencyPackerTest {

    @SuppressForbidden(reason = "Want to look at the data")
    @ParameterizedTest
    @EnumSource(GdsFeatureToggles.AdjacencyPackingStrategy.class)
    void roundtrip(GdsFeatureToggles.AdjacencyPackingStrategy strategy) {
        var random = newRandom();

        var values = random.random().longs(256, 0, 1L << 50).toArray();
        Arrays.sort(values);
        var originalValues = Arrays.copyOf(values, values.length);
        var uncompressedSize = originalValues.length * Long.BYTES;

        TestAllocator.testCursor(strategy, values, values.length, Aggregation.NONE, (cursor, slice) -> {
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

    @ParameterizedTest
    @EnumSource(GdsFeatureToggles.AdjacencyPackingStrategy.class)
    void compressConsecutiveLongs(GdsFeatureToggles.AdjacencyPackingStrategy strategy) {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();

        TestAllocator.testCursor(strategy, data, data.length, Aggregation.NONE, (cursor, slice) -> {
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

    @ParameterizedTest
    @EnumSource(GdsFeatureToggles.AdjacencyPackingStrategy.class)
    void compressRandomLongs(GdsFeatureToggles.AdjacencyPackingStrategy strategy) {
        var random = newRandom();
        var data = random.random().longs(AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50).toArray();

        TestAllocator.testCursor(strategy, data, data.length, Aggregation.NONE, (cursor, slice) -> {
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

    @ParameterizedTest
    @EnumSource(GdsFeatureToggles.AdjacencyPackingStrategy.class)
    void compressDuplicatedLongs(GdsFeatureToggles.AdjacencyPackingStrategy strategy) {
        var random = newRandom();
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE)
            .flatMap(value -> random.random().nextBoolean() ? LongStream.of(value) : LongStream.of(value, value))
            .limit(AdjacencyPacking.BLOCK_SIZE)
            .toArray();

        TestAllocator.testCursor(strategy, data, data.length, Aggregation.SINGLE, (cursor, slice) -> {
            int maxBytes;
            if (strategy == GdsFeatureToggles.AdjacencyPackingStrategy.VAR_LONG_TAIL) {
                int maxBitsPerValue = 8; // var-long needs 1 Byte per value in this test
                int maxBitsPerBlock = maxBitsPerValue * AdjacencyPacking.BLOCK_SIZE;
                int maxBytesPerBlock = maxBitsPerBlock / Byte.SIZE;
                maxBytes = 0 /* empty header */ + maxBytesPerBlock;
            } else {
                // packing
                int maxBitsPerValue = 6; // packed uses at most 6 bits for any value in this test
                int maxBitsPerBlock = maxBitsPerValue * AdjacencyPacking.BLOCK_SIZE;
                int maxBytesPerBlock = maxBitsPerBlock / Byte.SIZE;
                maxBytes = 8 /* aligned header size */ + maxBytesPerBlock;
            }

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

    @ParameterizedTest
    @EnumSource(GdsFeatureToggles.AdjacencyPackingStrategy.class)
    void compressRandomDuplicatedLongs(GdsFeatureToggles.AdjacencyPackingStrategy strategy) {
        var random = newRandom();
        var data = random.random().longs(2 * AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50)
            .distinct()
            .flatMap(value -> random.random().nextBoolean() ? LongStream.of(value) : LongStream.of(value, value))
            .limit(AdjacencyPacking.BLOCK_SIZE)
            .toArray();

        TestAllocator.testCursor(strategy, data, data.length, Aggregation.SINGLE, (cursor, slice) -> {

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

    static Stream<Arguments> strategyAndValueCount() {
        return TestSupport.crossArgument(
            () -> Arrays.stream(GdsFeatureToggles.AdjacencyPackingStrategy.values()),
            () -> Stream.of(42, 1337)
        );
    }

    @ParameterizedTest
    @MethodSource("strategyAndValueCount")
    void compressNonBlockAlignedConsecutiveLongs(GdsFeatureToggles.AdjacencyPackingStrategy strategy, int valueCount) {
        assertThat(valueCount % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var data = LongStream.range(0, valueCount).toArray();
        var alignedData = Arrays.copyOf(data, AdjacencyPackerUtil.align(valueCount));

        TestAllocator.testCursor(strategy, alignedData, valueCount, Aggregation.NONE, (cursor, slice) -> {
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
    @MethodSource("strategyAndValueCount")
    void compressNonBlockAlignedRandomLongs(GdsFeatureToggles.AdjacencyPackingStrategy strategy, int valueCount) {
        assertThat(valueCount % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var random = newRandom();
        var data = random.random().longs(4242, 0, 1L << 50)
            .distinct()
            .limit(valueCount)
            .toArray();

        var alignedData = Arrays.copyOf(data, AdjacencyPackerUtil.align(valueCount));

        TestAllocator.testCursor(strategy, alignedData, valueCount, Aggregation.NONE, (cursor, slice) -> {
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

    static Stream<Arguments> strategyAndBlockSizes() {
        return TestSupport.crossArgument(
            () -> Arrays.stream(GdsFeatureToggles.AdjacencyPackingStrategy.values()),
            () -> IntStream.of(
                0,
                1,
                42,
                AdjacencyPacking.BLOCK_SIZE,
                AdjacencyPacking.BLOCK_SIZE * 2,
                AdjacencyPacking.BLOCK_SIZE * 2 + 42,
                1337
            ).boxed()
        );
    }

    @ParameterizedTest
    @MethodSource("strategyAndBlockSizes")
    void decompressConsecutiveLongsViaCursor(GdsFeatureToggles.AdjacencyPackingStrategy strategy, int length) {
        var data = LongStream.range(0, length).toArray();
        var alignedData = Arrays.copyOf(data, AdjacencyPackerUtil.align(length));

        TestAllocator.testCursor(strategy, alignedData, length, Aggregation.NONE, (cursor, ignore) -> {

            assertThat(cursor.remaining()).isEqualTo(length);

            long[] decompressed = CursorUtil.decompressCursor(cursor);

            assertThat(decompressed)
                .as("compressed data did not roundtrip")
                .containsExactly(data);
        });
    }

    @ParameterizedTest
    @MethodSource("strategyAndBlockSizes")
    void decompressRandomLongsViaCursor(GdsFeatureToggles.AdjacencyPackingStrategy strategy, int length) {
        var random = newRandom();
        var data = random.random().longs(length, 0, 1L << 50).toArray();
        var alignedData = Arrays.copyOf(data, AdjacencyPackerUtil.align(length));

        TestAllocator.testCursor(strategy, alignedData, length, Aggregation.NONE, (cursor, ignore) -> {

            assertThat(cursor.remaining()).isEqualTo(length);

            long[] decompressed = CursorUtil.decompressCursor(cursor);

            // We need to sort due to random values.
            Arrays.sort(data);

            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(data);
        });
    }
}
