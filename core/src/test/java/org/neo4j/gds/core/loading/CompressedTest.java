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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.SeededRandom.newRandom;

class CompressedTest {

    static Stream<AdjacencyPackerTest.Features> cursorFeatures() {
        return Stream.of(AdjacencyPackerTest.Features.Sort, AdjacencyPackerTest.Features.SortAndDelta);
    }

    @ParameterizedTest
    @MethodSource("cursorFeatures")
    void decompressConsecutiveLongsViaCursor(AdjacencyPackerTest.Features features) {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        var adjacencyList = HugeObjectArray.of(compressed);
        var cursor = new DecompressingCursor(adjacencyList, features.flags());

        cursor.init(0, -1);

        var decompressed = new long[compressed.length()];

        var idx = 0;
        while (cursor.hasNextVLong()) {
            decompressed[idx++] = cursor.nextVLong();
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip")
            .containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @MethodSource("cursorFeatures")
    void decompressRandomLongsViaCursor(AdjacencyPackerTest.Features features) {
        var random = newRandom();
        var data = random.random().longs(AdjacencyPacking.BLOCK_SIZE, 0, 1L << 50).toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        var adjacencyList = HugeObjectArray.of(compressed);
        var cursor = new DecompressingCursor(adjacencyList, features.flags());

        cursor.init(0, -1);

        var decompressed = new long[compressed.length()];

        var idx = 0;
        while (cursor.hasNextVLong()) {
            decompressed[idx++] = cursor.nextVLong();
        }

        if (features != AdjacencyPackerTest.Features.Delta) {
            Arrays.sort(data);
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip, seed = %d", random.seed())
            .containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @MethodSource("cursorFeatures")
    void decompressNonBlockAlignedConsecutiveLongsViaCursor(AdjacencyPackerTest.Features features) {
        var length = 1337;
        assertThat(length % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var data = LongStream.range(0, length).toArray();

        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        var adjacencyList = HugeObjectArray.of(compressed);
        var cursor = new DecompressingCursor(adjacencyList, features.flags());

        cursor.init(0, -1);

        var decompressed = new long[compressed.length()];

        var idx = 0;
        while (cursor.hasNextVLong()) {
            decompressed[idx++] = cursor.nextVLong();
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip")
            .containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @MethodSource("cursorFeatures")
    void decompressNonBlockAlignedRandomLongsViaCursor(AdjacencyPackerTest.Features features) {
        var length = 1337;
        assertThat(length % AdjacencyPacking.BLOCK_SIZE).isNotEqualTo(0);
        var random = newRandom();
        var data = random.random().longs(4242, 0, 1L << 50)
            .distinct()
            .limit(length)
            .toArray();

        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        var adjacencyList = HugeObjectArray.of(compressed);
        var cursor = new DecompressingCursor(adjacencyList, features.flags());

        cursor.init(0, -1);

        var decompressed = new long[compressed.length()];

        var idx = 0;
        while (cursor.hasNextVLong()) {
            decompressed[idx++] = cursor.nextVLong();
        }

        if (features != AdjacencyPackerTest.Features.Delta) {
            Arrays.sort(data);
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip, seed = %d", random.seed())
            .containsExactly(data);

        compressed.free();
    }

}
