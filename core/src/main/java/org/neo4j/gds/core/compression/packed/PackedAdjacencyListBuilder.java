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
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

public final class PackedAdjacencyListBuilder implements AdjacencyListBuilder<Address, PackedAdjacencyList> {

    private final BumpAllocator<Address> builder;

    PackedAdjacencyListBuilder() {
        this.builder = new BumpAllocator<>(Factory.INSTANCE);
    }

    @Override
    public Allocator newAllocator() {
        return new Allocator(this.builder.newLocalAllocator());
    }

    @Override
    public PositionalAllocator<Address> newPositionalAllocator() {
        throw new UnsupportedOperationException("Packed adjacency lists do not support positional allocation.");
    }

    @Override
    public PackedAdjacencyList build(HugeIntArray degrees, HugeLongArray offsets) {
        Address[] intoPages = this.builder.intoPages();
        reorder(intoPages, offsets, degrees);
        long[] pages = new long[intoPages.length];
        int[] allocationSizes = new int[intoPages.length];
        for (int i = 0; i < intoPages.length; i++) {
            Address address = intoPages[i];
            pages[i] = address.address();
            allocationSizes[i] = Math.toIntExact(address.bytes());
        }
        return new PackedAdjacencyList(pages, allocationSizes, degrees, offsets);
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

        private Allocator(BumpAllocator.LocalAllocator<Address> allocator) {
            this.allocator = allocator;
        }

        @Override
        public long allocate(int length, Slice<Address> into) {
            return this.allocator.insertInto(length, (ModifiableSlice<Address>) into);
        }

        @Override
        public void close() {
        }
    }
}
