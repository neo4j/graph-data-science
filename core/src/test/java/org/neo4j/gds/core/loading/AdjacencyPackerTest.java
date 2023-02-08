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

import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.mem.BitUtil;

import java.util.Arrays;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdjacencyPackerTest {

    @SuppressForbidden(reason = "Want to look at the data")
    @Test
    void roundtrip() {
        var random = newRandom();

        var values = random.random().longs(256, 0, 1L << 50).toArray();
        Arrays.sort(values);
        var originalValues = Arrays.copyOf(values, values.length);
        var uncompressedSize = originalValues.length * Long.BYTES;

        var compressed = AdjacencyPacker.compress(values, 0, values.length, AdjacencyPacker.DELTA);
        var newRequiredBytes = compressed.bytesUsed();
        System.out.printf(
            Locale.ENGLISH,
            "new compressed = %d ratio = %.2f%n",
            newRequiredBytes,
            (double) newRequiredBytes / uncompressedSize
        );

        var decompressed = AdjacencyPacker.decompressAndPrefixSum(compressed);
        compressed.free();
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
            "dvl compressed = %d ratio = %.2f%n",
            requiredBytes,
            (double) requiredBytes / uncompressedSize
        );

        assertThat(newRequiredBytes)
            .as("new compressed should be less than dvl compressed, seed = %d", random.seed())
            .isLessThanOrEqualTo(requiredBytes);
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    void compressConsecutiveLongs(Features features) {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        assertThat(compressed.bytesUsed()).isLessThanOrEqualTo(1 + 6 * Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        var decompressed = features.decompress(compressed);
        assertThat(decompressed).containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    void compressRandomLongs(Features features) {
        var random = newRandom();
        var data = random.random().longs(AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50).toArray();

        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        assertThat(compressed.bytesUsed())
            .as("compressed exceeds original size, seed = %d", random.seed())
            .isLessThanOrEqualTo(1 + AdjacencyPacking.BLOCK_SIZE * Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        var decompressed = features.decompress(compressed);

        if (features != Features.Delta) {
            Arrays.sort(data);
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip, seed = %d", random.seed())
            .containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, mode = EnumSource.Mode.EXCLUDE, names = "Sort")
    void compressDeltaLongs(Features features) {
        var data = LongStream.rangeClosed(1, AdjacencyPacking.BLOCK_SIZE).toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        assertThat(compressed.bytesUsed()).isEqualTo(1 + Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        var decompressed = AdjacencyPacker.decompress(compressed);

        var deltas = new long[data.length];
        Arrays.fill(deltas, 1L);
        assertThat(decompressed).containsExactly(deltas);

        decompressed = AdjacencyPacker.decompressAndPrefixSum(compressed);
        assertThat(decompressed).containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @EnumSource(value = Features.class)
    void compressDuplicatedLongs(Features features) {
        var random = newRandom();
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE)
            .flatMap(value -> random.random().nextBoolean() ? LongStream.of(value) : LongStream.of(value, value))
            .limit(AdjacencyPacking.BLOCK_SIZE)
            .toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags(Aggregation.SINGLE));

        assertThat(compressed.bytesUsed()).isLessThanOrEqualTo(1 + 6 * Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        if (features != Features.Sort) {
            data = Arrays.stream(data).distinct().toArray();
        }

        var decompressed = features.decompress(compressed);
        assertThat(decompressed).containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @EnumSource(value = Features.class)
    void compressRandomDuplicatedLongs(Features features) {
        var random = newRandom();
        var data = random.random().longs(2 * AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50)
            .distinct()
            .flatMap(value -> random.random().nextBoolean() ? LongStream.of(value) : LongStream.of(value, value))
            .limit(AdjacencyPacking.BLOCK_SIZE)
            .toArray();

        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags(Aggregation.SINGLE));

        assertThat(compressed.bytesUsed())
            .as("compressed exceeds original size, seed = %d", random.seed())
            .isLessThanOrEqualTo(1 + AdjacencyPacking.BLOCK_SIZE * Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        var decompressed = features.decompress(compressed);

        switch (features) {
            case Sort:
                Arrays.sort(data);
                break;
            case Delta:
                var lastValue = new MutableObject<>(OptionalLong.empty());
                data = Arrays.stream(data).filter(l -> {
                    var last = lastValue.getValue();
                    if (last.isEmpty() || last.getAsLong() < l) {
                        lastValue.setValue(OptionalLong.of(l));
                        return true;
                    }
                    return false;
                }).toArray();
                break;
            case SortAndDelta:
                Arrays.sort(data);
                data = Arrays.stream(data).distinct().toArray();
                break;
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip, seed = %d", random.seed())
            .containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @EnumSource(value = Features.class)
    void compressNonBlockAlignedConsecutiveLongs(Features features) {
        assertThat(1337 % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var data = LongStream.range(0, 1337).toArray();

        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        assertThat(compressed.bytesUsed())
            .as("compressed exceeds original size")
            .isLessThanOrEqualTo(1337L * Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        var decompressed = features.decompress(compressed);

        assertThat(decompressed)
            .as("compressed data did not roundtrip")
            .containsExactly(data);

        compressed.free();
    }

    @Test
    void compressNonBlockAlignedRandomLongs() {
        assertThat(1337 % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var random = newRandom();
        var data = random.random().longs(4242, 0, 1L << 50)
            .distinct()
            .limit(1337)
            .toArray();

        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, Features.SortAndDelta.flags());

        assertThat(compressed.bytesUsed())
            .as("compressed exceeds original size, seed = %d", random.seed())
            .isLessThanOrEqualTo(1337L * Long.BYTES);
        assertThat(compressed.address()).isNotZero();

        var decompressed = Features.SortAndDelta.decompress(compressed);

        Arrays.sort(data);

        assertThat(decompressed)
            .as("compressed data did not roundtrip, seed = %d", random.seed())
            .containsExactly(data);

        compressed.free();
    }

    @Test
    void preventDoubleFree() {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
        var compressed = AdjacencyPacker.compress(data, 0, data.length);
        assertThatCode(compressed::free).doesNotThrowAnyException();
        assertThatThrownBy(compressed::free)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("This compressed memory has already been freed.");
    }

    @Test
    void preventUseAfterFree() {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
        var compressed = AdjacencyPacker.compress(data, 0, data.length);
        assertThatCode(compressed::free).doesNotThrowAnyException();
        assertThatThrownBy(() -> AdjacencyPacker.decompress(compressed))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("This compressed memory has already been freed.");
    }

    enum Features {
        Sort,
        Delta,
        SortAndDelta;

        int flags() {
            switch (this) {
                case Sort:
                    return AdjacencyPacker.SORT;
                case Delta:
                    return AdjacencyPacker.DELTA;
                case SortAndDelta:
                    return AdjacencyPacker.SORT | AdjacencyPacker.DELTA;
                default:
                    throw new IllegalArgumentException();
            }
        }

        int flags(Aggregation aggregation) {
            return flags() | aggregation.ordinal();
        }

        long[] decompress(Compressed compressed) {
            switch (this) {
                case Sort:
                    return AdjacencyPacker.decompress(compressed);
                case Delta:
                case SortAndDelta:
                    return AdjacencyPacker.decompressAndPrefixSum(compressed);
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    private static SeededRandom newRandom() {
        var random = new Random();
        var seed = random.nextLong();
        random.setSeed(seed);
        return ImmutableSeededRandom.of(random, seed);
    }

    @ValueClass
    interface SeededRandom {
        Random random();

        long seed();
    }
}
