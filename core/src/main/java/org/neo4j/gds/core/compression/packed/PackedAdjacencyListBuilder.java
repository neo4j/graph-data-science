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

import java.util.Arrays;

public final class PackedAdjacencyListBuilder implements AdjacencyListBuilder<Long, PackedAdjacencyList> {

    private final BumpAllocator<Long> builder;

    public PackedAdjacencyListBuilder() {
        this.builder = new BumpAllocator<>(Factory.INSTANCE);
    }

    @Override
    public Allocator newAllocator() {
        return new Allocator(this.builder.newLocalAllocator());
    }

    @Override
    public PositionalAllocator<Long> newPositionalAllocator() {
        throw new UnsupportedOperationException("Packed adjacency lists do not support positional allocation.");
    }

    @Override
    public PackedAdjacencyList build(HugeIntArray degrees, HugeLongArray offsets) {
        Long[] intoPages = this.builder.intoPages();
        reorder(intoPages, offsets, degrees);
        long[] pages = new long[intoPages.length];
        Arrays.setAll(pages, i -> intoPages[i]);
        return new PackedAdjacencyList(pages, degrees, offsets);
    }

    private enum Factory implements BumpAllocator.Factory<Long> {
        INSTANCE;

        @Override
        public Long[] newEmptyPages() {
            return new Long[0];
        }

        @Override
        public Long newPage(int length) {
            return UnsafeUtil.allocateMemory(length, EmptyMemoryTracker.INSTANCE);
        }
    }

    static final class Allocator implements AdjacencyListBuilder.Allocator<Long> {

        private final BumpAllocator.LocalAllocator<Long> allocator;

        private Allocator(BumpAllocator.LocalAllocator<Long> allocator) {
            this.allocator = allocator;
        }

        @Override
        public long allocate(int length, Slice<Long> into) {
            return this.allocator.insertInto(length, (ModifiableSlice<Long>) into);
        }

        @Override
        public void close() {
        }
    }
}
