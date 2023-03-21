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

import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

class TestAllocator implements AdjacencyListBuilder.Allocator<Long> {
    private Address address;

    public static void test(BiConsumer<AdjacencyListBuilder.Allocator<Long>, AdjacencyListBuilder.Slice<Long>> code) {
        try (var allocator = new TestAllocator()) {
            var slice = ModifiableSlice.<Long>create();
            code.accept(allocator, slice);
        }
    }

    public static void testList(
        long[] values,
        int length,
        Aggregation aggregation,
        Consumer<PackedAdjacencyList> code
    ) {
        test((allocator, slice) -> {
            var offset = AdjacencyPacker2.compress(
                allocator,
                slice,
                values,
                length,
                aggregation
            );

            var pages = new long[]{slice.slice()};
            var degrees = HugeIntArray.of(slice.length());
            var offsets = HugeLongArray.of(offset);
            var list = new PackedAdjacencyList(pages, degrees, offsets);

            code.accept(list);
        });
    }

    public static void testCursor(
        long[] values,
        int length,
        Aggregation aggregation,
        Consumer<DecompressingCursor> code
    ) {
        test((allocator, slice) -> {
            var offset = AdjacencyPacker2.compress(
                allocator,
                slice,
                values,
                length,
                aggregation
            );

            var pages = new long[]{slice.slice()};
            var cursor = new DecompressingCursor(pages);
            cursor.init(offset, slice.length());

            code.accept(cursor);
        });
    }

    @Override
    public long allocate(int length, AdjacencyListBuilder.Slice<Long> into) {
        var slice = (ModifiableSlice<Long>) into;
        long ptr = UnsafeUtil.allocateMemory(length, EmptyMemoryTracker.INSTANCE);
        this.address = Address.createAddress(ptr, length);
        slice.setSlice(ptr);
        slice.setOffset(0);
        slice.setLength(length);
        return 0;
    }

    @Override
    public void close() {
        // deallocate
        this.address.run();
    }
}
