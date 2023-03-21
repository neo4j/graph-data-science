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
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.Aggregation;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.SeededRandom.newRandom;

class CompressedTest {

    @ParameterizedTest
    @ValueSource(ints = {
        0,
        1,
        42,
        AdjacencyPacking.BLOCK_SIZE,
        AdjacencyPacking.BLOCK_SIZE * 2,
        AdjacencyPacking.BLOCK_SIZE * 2 + 42,
        1337
    })
    void decompressConsecutiveLongsViaCursor(int length) {
        var data = LongStream.range(0, length).toArray();
        var alignedData = Arrays.copyOf(data, AdjacencyPacker.align(length));

        TestAllocator.testCursor(alignedData, length, Aggregation.NONE, cursor -> {

            assertThat(cursor.remaining()).isEqualTo(length);

            long[] decompressed = decompressCursor(length, cursor);

            assertThat(decompressed)
                .as("compressed data did not roundtrip")
                .containsExactly(data);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {
        0,
        1,
        42,
        AdjacencyPacking.BLOCK_SIZE,
        AdjacencyPacking.BLOCK_SIZE * 2,
        AdjacencyPacking.BLOCK_SIZE * 2 + 42,
        1337
    })
    void decompressRandomLongsViaCursor(int length) {
        var random = newRandom();
        var data = random.random().longs(length, 0, 1L << 50).toArray();
        Arrays.sort(data);
        var alignedData = Arrays.copyOf(data, AdjacencyPacker.align(length));

        TestAllocator.testCursor(alignedData, length, Aggregation.NONE, cursor -> {

            assertThat(cursor.remaining()).isEqualTo(length);

            long[] decompressed = decompressCursor(length, cursor);

            assertThat(decompressed)
                .as("compressed data did not roundtrip, seed = %d", random.seed())
                .containsExactly(data);
        });
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
