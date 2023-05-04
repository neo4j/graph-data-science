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

import org.HdrHistogram.ConcurrentHistogram;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PackedAdjacencyListTest {

    @Test
    void preventUseAfterFree() {
        var list = adjacencyList(LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray());

        assertThatCode(list::free).doesNotThrowAnyException();
        assertThatThrownBy(() -> list.adjacencyCursor(0, 42.1337))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("This page has already been freed.");
    }

    @Test
    void memoryInfo() {
        var list = adjacencyList(LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray());

        var memoryInfo = list.memoryInfo();

        assertThat(memoryInfo.bytesTotal()).isPresent();
        assertThat(memoryInfo.bytesOffHeap()).isPresent();
        assertThat(memoryInfo.bytesOnHeap()).isPresent();
        assertThat(memoryInfo.bytesTotal().getAsLong()).isGreaterThan(0L);
        assertThat(memoryInfo.bytesOnHeap().getAsLong()).isGreaterThan(0L);
        assertThat(memoryInfo.bytesOffHeap().getAsLong()).isGreaterThan(0L);
    }

    private static PackedAdjacencyList adjacencyList(long[] data) {
        var allocator = new TestAllocator();
        var slice = ModifiableSlice.<Address>create();
        var degree = new MutableInt(0);
        var offset = AdjacencyPacker.compressWithVarLongTail(
            allocator,
            slice,
            data,
            data.length,
            Aggregation.NONE,
            degree,
            null,
            null
        );

        long ptr = slice.slice().address();
        var pages = new long[]{ptr};
        var bytesOffHeap = Math.toIntExact(slice.slice().bytes());
        var allocationSizes = new int[]{bytesOffHeap};
        var degrees = HugeIntArray.of(degree.intValue());
        var offsets = HugeLongArray.of(offset);

        return new PackedAdjacencyList(pages, allocationSizes, degrees, offsets, new ConcurrentHistogram(0));
    }
}
