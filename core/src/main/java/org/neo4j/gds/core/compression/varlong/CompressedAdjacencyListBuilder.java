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
package org.neo4j.gds.core.compression.varlong;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.AdjacencyList.MemoryInfo;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.compression.common.MemoryTracker;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;

public final class CompressedAdjacencyListBuilder implements AdjacencyListBuilder<byte[], CompressedAdjacencyList> {

    private final BumpAllocator<byte[]> builder;
    private final MemoryTracker memoryTracker;

    CompressedAdjacencyListBuilder(MemoryTracker memoryTracker) {
        this.builder = new BumpAllocator<>(Factory.INSTANCE);
        this.memoryTracker = memoryTracker;
    }

    @Override
    public Allocator newAllocator() {
        return new Allocator(this.builder.newLocalAllocator(), this.memoryTracker);
    }

    @Override
    public PositionalAllocator<byte[]> newPositionalAllocator() {
        throw new UnsupportedOperationException("Compressed adjacency lists do not support positional allocation.");
    }

    @Override
    public CompressedAdjacencyList build(HugeIntArray degrees, HugeLongArray offsets) {
        byte[][] intoPages = builder.intoPages();
        reorder(intoPages, offsets, degrees);
        var memoryInfo = memoryInfo(intoPages, degrees, offsets);

        return new CompressedAdjacencyList(intoPages, degrees, offsets, memoryInfo);
    }

    private MemoryInfo memoryInfo(byte[][] pages, HugeIntArray degrees, HugeLongArray offsets) {
        for (byte[] page : pages) {
            this.memoryTracker.recordPageSize(page.length * Byte.BYTES);
        }

        var memoryInfoBuilder = MemoryInfo
            .builder(memoryTracker)
            .pages(pages.length)
            .bytesOffHeap(0);

        var sizeOnHeap = new MutableLong();
        MemoryUsage.sizeOfObject(pages).ifPresent(sizeOnHeap::add);
        MemoryUsage.sizeOfObject(degrees).ifPresent(sizeOnHeap::add);
        MemoryUsage.sizeOfObject(offsets).ifPresent(sizeOnHeap::add);
        memoryInfoBuilder.bytesOnHeap(sizeOnHeap.longValue());

        return memoryInfoBuilder.build();
    }

    enum Factory implements BumpAllocator.Factory<byte[]> {
        INSTANCE;

        @Override
        public byte[][] newEmptyPages() {
            return new byte[0][];
        }

        @Override
        public byte[] newPage(int length) {
            return new byte[length];
        }
    }

    static final class Allocator implements AdjacencyListBuilder.Allocator<byte[]> {

        private final BumpAllocator.LocalAllocator<byte[]> allocator;
        private final MemoryTracker memoryTracker;

        private Allocator(BumpAllocator.LocalAllocator<byte[]> allocator, MemoryTracker memoryTracker) {
            this.allocator = allocator;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public long allocate(int allocationSize, Slice<byte[]> into) {
            this.memoryTracker.recordHeapAllocation(allocationSize);
            return allocator.insertInto(allocationSize, (ModifiableSlice<byte[]>) into);
        }

        @Override
        public void close() {
        }
    }
}
