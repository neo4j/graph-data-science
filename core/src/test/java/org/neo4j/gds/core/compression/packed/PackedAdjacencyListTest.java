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

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PackedAdjacencyListTest {

    @Test
    void preventUseAfterFree() {
        var data = LongStream.range(0, AdjacencyPacking.BLOCK_SIZE).toArray();
        var allocator = new TestAllocator();
        var slice = ModifiableSlice.<Address>create();
        var degree = new MutableInt(0);
        var offset = AdjacencyPacker.compress(
            allocator,
            slice,
            data,
            data.length,
            Aggregation.NONE,
            degree
        );

        long ptr = slice.slice().address();
        var pages = new long[]{ptr};
        var allocationSizes = new int[]{Math.toIntExact(slice.slice().bytes())};
        var degrees = HugeIntArray.of(degree.intValue());
        var offsets = HugeLongArray.of(offset);
        var list = new PackedAdjacencyList(pages, allocationSizes, degrees, offsets);

        assertThatCode(list::free).doesNotThrowAnyException();
        assertThatThrownBy(() -> list.adjacencyCursor(0, 42.1337))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("This page has already been freed.");
    }
}
