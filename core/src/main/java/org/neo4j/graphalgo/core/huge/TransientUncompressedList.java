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
package org.neo4j.graphalgo.core.huge;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyProperties;
import org.neo4j.graphalgo.api.PropertyCursor;
import org.neo4j.graphalgo.core.loading.MutableIntValue;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.core.loading.BumpAllocator.PAGE_MASK;
import static org.neo4j.graphalgo.core.loading.BumpAllocator.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.loading.BumpAllocator.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.PageUtil.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.PageUtil.pageIndex;

public final class TransientUncompressedList implements AdjacencyList, AdjacencyProperties {

    public static MemoryEstimation uncompressedMemoryEstimation(RelationshipType relationshipType, boolean undirected) {
        return MemoryEstimations
            .builder(TransientUncompressedList.class)
            .perGraphDimension("pages", (dimensions, concurrency) -> {
                long nodeCount = dimensions.nodeCount();
                long relCountForType = dimensions.relationshipCounts().getOrDefault(relationshipType, dimensions.maxRelCount());
                long relCount = undirected ? relCountForType * 2 : relCountForType;

                long uncompressedAdjacencySize = relCount * Long.BYTES + nodeCount * Integer.BYTES;
                int pages = PageUtil.numPagesFor(uncompressedAdjacencySize, PAGE_SHIFT, PAGE_MASK);
                long bytesPerPage = MemoryUsage.sizeOfByteArray(PAGE_SIZE);

                return MemoryRange.of(pages * bytesPerPage + MemoryUsage.sizeOfObjectArray(pages));
            })
            /*
             NOTE: one might think to follow this with something like

            .perNode("degrees", HugeIntArray::memoryEstimation)

             This is the estimation for the property implementation which shares the actual
             degree data with the adjacency list. We only need to count the reference, which is already accounted for.
             */
            .perNode("offsets", HugeLongArray::memoryEstimation)
            .build();
    }

    @TestOnly
    public static MemoryEstimation uncompressedMemoryEstimation(boolean undirected) {
        return uncompressedMemoryEstimation(ALL_RELATIONSHIPS, undirected);
    }

    private long[][] pages;
    private HugeIntArray degrees;
    private HugeLongArray offsets;

    public TransientUncompressedList(long[][] pages, HugeIntArray degrees, HugeLongArray offsets) {
        this.pages = pages;
        this.degrees = degrees;
        this.offsets = offsets;
    }

    @Override
    public int degree(long node) {
        return degrees.get(node);
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        var degree = degrees.get(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }
        var cursor = new UncompressedCursor(pages);
        var offset = offsets.get(node);
        cursor.init(offset, degree);
        return cursor;
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return null;
    }

    @Override
    public void close() {
        pages = null;
        degrees = null;
        offsets = null;
    }

    @Override
    public PropertyCursor propertyCursor(long node, double fallbackValue) {
        var degree = degrees.get(node);
        if (degree == 0) {
            return PropertyCursor.empty();
        }
        var cursor = new Cursor(pages);
        var offset = offsets.get(node);
        cursor.init(offset, degree);
        return cursor;
    }

    @Override
    public PropertyCursor propertyCursor(PropertyCursor reuse, long node, double fallbackValue) {
        if (reuse instanceof Cursor) {
            return reuse.init(offsets.get(node), degrees.get(node));
        }
        return propertyCursor(node, fallbackValue);
    }

    private static final class Cursor implements PropertyCursor {

        private long[][] pages;

        private long[] currentPage;
        private int offset;
        private int limit;

        private Cursor(long[][] pages) {
            this.pages = pages;
        }

        @Override
        public boolean hasNextLong() {
            return offset < limit;
        }

        @Override
        public long nextLong() {
            return currentPage[offset++];
        }

        @Override
        public Cursor init(long fromIndex, int degree) {
            this.currentPage = pages[pageIndex(fromIndex, PAGE_SHIFT)];
            this.offset = indexInPage(fromIndex, PAGE_MASK);
            this.limit = offset + degree;
            return this;
        }

        @Override
        public void close() {
            pages = null;
        }
    }

    public static final class UncompressedCursor extends MutableIntValue implements AdjacencyCursor {

        private long[][] pages;

        private long[] currentPage;
        private int degree;
        private int limit;
        private int offset;

        private UncompressedCursor(long[][] pages) {
            this.pages = pages;
        }

        @Override
        public void init(long fromIndex, int degree) {
            currentPage = pages[pageIndex(fromIndex, PAGE_SHIFT)];
            offset = indexInPage(fromIndex, PAGE_MASK);
            limit = offset + degree * Long.BYTES;
            this.degree = degree;
        }

        @Override
        public int size() {
            return degree;
        }

        @Override
        public int remaining() {
            return limit - offset;
        }

        @Override
        public boolean hasNextVLong() {
            return offset < limit;
        }

        @Override
        public long nextVLong() {
            return currentPage[offset++];
        }

        @Override
        public long peekVLong() {
            return currentPage[offset];
        }

        @Override
        public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
            throw new UnsupportedOperationException();
//            var dest = destination instanceof DecompressingCursor
//                ? (DecompressingCursor) destination
//                : new DecompressingCursor(pages);
//            dest.decompress.copyFrom(this.decompress);
//            dest.currentPosition = this.currentPosition;
//            dest.maxTargets = this.maxTargets;
//            return dest;
        }

        // TODO: I think this documentation if either out of date or misleading.
        //  Either we skip all blocks and return -1 or we find a value that is strictly larger.
        /**
         * Read and decode target ids until it is strictly larger than ({@literal >}) the provided {@code target}.
         * Might return an id that is less than or equal to {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code skipUntil(target) <= target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        @Override
        public long skipUntil(long target) {
            throw new UnsupportedOperationException();
//            long value = decompress.skipUntil(target, remaining(), this);
//            this.currentPosition += this.value;
//            return value;
        }

        /**
         * Read and decode target ids until it is larger than or equal ({@literal >=}) the provided {@code target}.
         * Might return an id that is less than {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code advance(target) < target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        @Override
        public long advance(long target) {
            throw new UnsupportedOperationException();
//            int targetsLeftToBeDecoded = remaining();
//            if(targetsLeftToBeDecoded <= 0) {
//                return AdjacencyCursor.NOT_FOUND;
//            }
//            long value = decompress.advance(target, targetsLeftToBeDecoded, this);
//            this.currentPosition += this.value;
//            return value;
        }

        @Override
        public void close() {
            pages = null;
            currentPage = null;
        }
    }
}
