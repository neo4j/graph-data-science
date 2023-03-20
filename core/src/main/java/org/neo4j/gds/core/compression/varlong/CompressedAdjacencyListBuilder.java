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

import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Arrays;

public final class CompressedAdjacencyListBuilder implements AdjacencyListBuilder<byte[], CompressedAdjacencyList> {

    private final BumpAllocator<byte[]> builder;

    CompressedAdjacencyListBuilder() {
        this.builder = new BumpAllocator<>(Factory.INSTANCE);
    }

    @Override
    public Allocator newAllocator() {
        return new Allocator(this.builder.newLocalAllocator());
    }

    @Override
    public PositionalAllocator<byte[]> newPositionalAllocator() {
        throw new UnsupportedOperationException("Compressed adjacency lists do not support positional allocation.");
    }

    @Override
    public CompressedAdjacencyList build(HugeIntArray degrees, HugeLongArray offsets) {
        var intoPages = builder.intoPages();
        reorder(intoPages, offsets, degrees);
        return new CompressedAdjacencyList(intoPages, degrees, offsets);
    }

    private enum Factory implements BumpAllocator.Factory<byte[]> {
        INSTANCE;

        @Override
        public byte[][] newEmptyPages() {
            return new byte[0][];
        }

        @Override
        public byte[] newPage(int length) {
            return new byte[length];
        }

        @Override
        public byte[] copyOfPage(byte[] bytes, int length) {
            return Arrays.copyOf(bytes, length);
        }

        @Override
        public int lengthOfPage(byte[] bytes) {
            return bytes.length;
        }
    }

    static final class Allocator implements AdjacencyListBuilder.Allocator<byte[]> {

        private final BumpAllocator.LocalAllocator<byte[]> allocator;

        private Allocator(BumpAllocator.LocalAllocator<byte[]> allocator) {
            this.allocator = allocator;
        }

        @Override
        public long allocate(int length, Slice<byte[]> into) {
            return allocator.insertInto(length, (ModifiableSlice<byte[]>) into);
        }

        @Override
        public void close() {
        }
    }
}
