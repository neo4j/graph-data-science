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
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.MemoryTracker;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.function.BiConsumer;

class TestAllocator implements AdjacencyListBuilder.Allocator<Address> {
    private Address address;

    public static void test(BiConsumer<AdjacencyListBuilder.Allocator<Address>, AdjacencyListBuilder.Slice<Address>> code) {
        try (var allocator = new TestAllocator()) {
            var slice = ModifiableSlice.<Address>create();
            code.accept(allocator, slice);
        }
    }

    public static void testCursor(
        long[] values,
        int length,
        Aggregation aggregation,
        BiConsumer<AdjacencyCursor, AdjacencyListBuilder.Slice<Address>> code
    ) {
        testCursor(GdsFeatureToggles.ADJACENCY_PACKING_STRATEGY.get(), values, length, aggregation, code);
    }

    public static void testCursor(
        GdsFeatureToggles.AdjacencyPackingStrategy adjacencyPackingStrategy,
        long[] values,
        int length,
        Aggregation aggregation,
        BiConsumer<AdjacencyCursor, AdjacencyListBuilder.Slice<Address>> code
    ) {
        test((allocator, slice) -> {
            var degree = new MutableInt();
            long offset;
            AdjacencyCursor cursor;

            switch (adjacencyPackingStrategy) {
                case VAR_LONG_TAIL:
                    offset = VarLongTailPacker.compress(
                        allocator,
                        slice,
                        values.clone(),
                        length,
                        aggregation,
                        degree,
                        MemoryTracker.empty()
                    );
                    cursor = new VarLongTailCursor(new long[]{slice.slice().address()});
                    break;
                case PACKED_TAIL:
                    offset = PackedTailPacker.compress(
                        allocator,
                        slice,
                        values.clone(),
                        length,
                        aggregation,
                        degree,
                        MemoryTracker.empty()
                    );
                    cursor = new PackedTailCursor(new long[]{slice.slice().address()});
                    break;
                case BLOCK_ALIGNED_TAIL:
                    offset = BlockAlignedTailPacker.compress(allocator,
                        slice,
                        values.clone(),
                        length,
                        aggregation,
                        degree
                    );
                    cursor = new BlockAlignedTailCursor(new long[]{slice.slice().address()});
                    break;
                case INLINED_HEAD_PACKED_TAIL:
                    offset = InlinedHeadPackedTailPacker.compress(
                        allocator,
                        slice,
                        values.clone(),
                        length,
                        aggregation,
                        degree,
                        MemoryTracker.empty()
                    );
                    cursor = new InlinedHeadPackedTailCursor(new long[]{slice.slice().address()});
                    break;
                default:
                    throw new IllegalArgumentException("Unknown compression type" + adjacencyPackingStrategy);
            }
            cursor.init(offset, degree.intValue());
            code.accept(cursor, slice);
        });
    }

    @Override
    public long allocate(int length, AdjacencyListBuilder.Slice<Address> into) {
        var slice = (ModifiableSlice<Address>) into;
        long ptr = UnsafeUtil.allocateMemory(length, EmptyMemoryTracker.INSTANCE);
        this.address = Address.createAddress(ptr, length);
        slice.setSlice(address);
        slice.setOffset(0);
        slice.setLength(length);
        return 0;
    }

    @Override
    public void close() {
        this.address.free();
    }
}
