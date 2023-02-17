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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.SeededRandom.newRandom;

class CompressedTest {

    static Stream<Arguments> cursorFeaturesAndLengths() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                AdjacencyPackerTest.Features.Sort,
                AdjacencyPackerTest.Features.SortAndDelta
            ).map(Arguments::of),
            () -> Stream.of(
                0,
                1,
                42,
                AdjacencyPacking.BLOCK_SIZE,
                AdjacencyPacking.BLOCK_SIZE * 2,
                AdjacencyPacking.BLOCK_SIZE * 2 + 42,
                1337
            ).map(Arguments::of)
        );
    }

    @ParameterizedTest
    @MethodSource("cursorFeaturesAndLengths")
    void decompressConsecutiveLongsViaCursor(AdjacencyPackerTest.Features features, long length) {
        var data = LongStream.range(0, length).toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        var adjacencyList = HugeObjectArray.of(compressed);
        var cursor = new DecompressingCursor(adjacencyList, features.flags());

        cursor.init(0, -1);

        long[] decompressed = decompressCursor(compressed.length(), cursor);

        assertThat(decompressed)
            .as("compressed data did not roundtrip")
            .containsExactly(data);

        compressed.free();
    }

    @ParameterizedTest
    @MethodSource("cursorFeaturesAndLengths")
    void decompressRandomLongsViaCursor(AdjacencyPackerTest.Features features, long length) {
        var random = newRandom();
        var data = random.random().longs(length, 0, 1L << 50).toArray();
        var compressed = AdjacencyPacker.compress(data.clone(), 0, data.length, features.flags());

        var adjacencyList = HugeObjectArray.of(compressed);
        var cursor = new DecompressingCursor(adjacencyList, features.flags());

        cursor.init(0, -1);

        long[] decompressed = decompressCursor(compressed.length(), cursor);

        if (features != AdjacencyPackerTest.Features.Delta) {
            Arrays.sort(data);
        }

        assertThat(decompressed)
            .as("compressed data did not roundtrip, seed = %d", random.seed())
            .containsExactly(data);

        compressed.free();
    }

    private static long[] decompressCursor(int length, AdjacencyCursor cursor) {
        var decompressed = new long[length];

        var idx = 0;
        while (cursor.hasNextVLong()) {
            long peek = cursor.peekVLong();
            long next = cursor.nextVLong();
            assertThat(peek).isEqualTo(next);
            decompressed[idx++] = next;
        }
        return decompressed;
    }

}
