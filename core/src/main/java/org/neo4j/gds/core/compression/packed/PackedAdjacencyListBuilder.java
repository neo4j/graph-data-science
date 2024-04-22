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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.compression.MemoryInfo;
import org.neo4j.gds.core.compression.MemoryInfoUtil;
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.compression.common.MemoryTracker;
import org.neo4j.gds.mem.JolMemoryUsage;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.Arrays;
import java.util.Optional;

public final class PackedAdjacencyListBuilder implements AdjacencyListBuilder<Address, PackedAdjacencyList> {

    private final BumpAllocator<Address> builder;
    private final MemoryTracker memoryTracker;

    PackedAdjacencyListBuilder(MemoryTracker memoryTracker) {
        this.builder = new BumpAllocator<>(Factory.INSTANCE);
        this.memoryTracker = memoryTracker;
    }

    @Override
    public Allocator newAllocator() {
        return new Allocator(this.builder.newLocalAllocator(), this.memoryTracker);
    }

    @Override
    public PositionalAllocator<Address> newPositionalAllocator() {
        throw new UnsupportedOperationException("Packed adjacency lists do not support positional allocation.");
    }

    @Override
    public PackedAdjacencyList build(HugeIntArray degrees, HugeLongArray offsets, boolean allowReordering) {
        Address[] intoPages = this.builder.intoPages();
        if (allowReordering) {
            reorder(intoPages, offsets, degrees);
        }
        long[] pages = new long[intoPages.length];
        int[] allocationSizes = new int[intoPages.length];
        for (int i = 0; i < intoPages.length; i++) {
            Address address = intoPages[i];
            pages[i] = address.address();
            int allocationSize = Math.toIntExact(address.bytes());
            allocationSizes[i] = allocationSize;
        }

        var memoryInfo = memoryInfo(allocationSizes, degrees, offsets);

        return new PackedAdjacencyList(pages, allocationSizes, degrees, offsets, memoryInfo);
    }

    private MemoryInfo memoryInfo(int[] allocationSizes, HugeIntArray degrees, HugeLongArray offsets) {
        long bytesOffHeap = Arrays.stream(allocationSizes).peek(this.memoryTracker::recordPageSize).asLongStream().sum();

        var memoryInfoBuilder = MemoryInfoUtil
            .builder(memoryTracker, Optional.of(this.memoryTracker.blockStatistics()))
            .pages(allocationSizes.length)
            .bytesOffHeap(bytesOffHeap);

        var sizeOnHeap = new MutableLong();
        JolMemoryUsage.sizeOfObject(degrees).ifPresent(sizeOnHeap::add);
        JolMemoryUsage.sizeOfObject(offsets).ifPresent(sizeOnHeap::add);
        memoryInfoBuilder.bytesOnHeap(sizeOnHeap.longValue());

        return memoryInfoBuilder.build();
    }

    private enum Factory implements BumpAllocator.Factory<Address> {
        INSTANCE;

        @Override
        public Address[] newEmptyPages() {
            return new Address[0];
        }

        @Override
        public Address newPage(int length) {
            long ptr = UnsafeUtil.allocateMemory(length, EmptyMemoryTracker.INSTANCE);
            return Address.createAddress(ptr, length);
        }
    }

    static final class Allocator implements AdjacencyListBuilder.Allocator<Address> {

        private final BumpAllocator.LocalAllocator<Address> allocator;
        private final MemoryTracker memoryTracker;

        private Allocator(BumpAllocator.LocalAllocator<Address> allocator, MemoryTracker memoryTracker) {
            this.allocator = allocator;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public long allocate(int allocationSize, Slice<Address> into) {
            this.memoryTracker.recordNativeAllocation(allocationSize);
            return this.allocator.insertInto(allocationSize, (ModifiableSlice<Address>) into);
        }

        @Override
        public void close() {
        }
    }
}
