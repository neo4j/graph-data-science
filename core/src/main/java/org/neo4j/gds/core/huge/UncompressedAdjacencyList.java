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
package org.neo4j.gds.core.huge;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.PropertyCursor;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.BumpAllocator;
import org.neo4j.gds.core.loading.MutableIntValue;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.MemoryUsage;

import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.collections.PageUtil.indexInPage;
import static org.neo4j.gds.collections.PageUtil.pageIndex;

public final class UncompressedAdjacencyList implements AdjacencyList, AdjacencyProperties {

    public static MemoryEstimation adjacencyListEstimation(RelationshipType relationshipType, boolean undirected) {
        return MemoryEstimations.setup(
            "",
            dimensions -> UncompressedAdjacencyList.adjacencyListEstimation(
                averageDegree(dimensions, relationshipType, undirected),
                dimensions.nodeCount()
            )
        );
    }

    @TestOnly
    public static MemoryEstimation adjacencyListEstimation(boolean undirected) {
        return adjacencyListEstimation(ALL_RELATIONSHIPS, undirected);
    }

    public static MemoryEstimation adjacencyListEstimation(long avgDegree, long nodeCount) {
        return MemoryEstimations
            .builder(UncompressedAdjacencyList.class)
            .fixed("pages", listSize(avgDegree, nodeCount))
            .perNode("degrees", HugeIntArray::memoryEstimation)
            .perNode("offsets", HugeLongArray::memoryEstimation)
            .build();
    }

    public static MemoryEstimation adjacencyPropertiesEstimation(
        RelationshipType relationshipType,
        boolean undirected
    ) {
        return MemoryEstimations
            .builder(UncompressedAdjacencyList.class)
            .perGraphDimension("pages", (dimensions, concurrency) ->
                listSize(averageDegree(dimensions, relationshipType, undirected), dimensions.nodeCount())
            )
            /*
             NOTE: one might think to follow this with something like

            .perNode("degrees", HugeIntArray::memoryEstimation)

             This is the estimation for the property implementation which shares the actual
             degree data with the adjacency list. We only need to count the reference, which is already accounted for.
             */
            .perNode("offsets", HugeLongArray::memoryEstimation)
            .build();
    }

    private static long averageDegree(
        GraphDimensions dimensions,
        RelationshipType relationshipType,
        boolean undirected
    ) {
        long nodeCount = dimensions.nodeCount();
        long relCountForType = dimensions.relationshipCounts().getOrDefault(relationshipType, dimensions.relCountUpperBound());
        long relCount = undirected ? relCountForType * 2 : relCountForType;
        return (nodeCount > 0) ? BitUtil.ceilDiv(relCount, nodeCount) : 0L;
    }

    private static MemoryRange listSize(long avgDegree, long nodeCount) {
        long uncompressedAdjacencySize = nodeCount * avgDegree * Long.BYTES;
        int pages = PageUtil.numPagesFor(uncompressedAdjacencySize, BumpAllocator.PAGE_SHIFT, BumpAllocator.PAGE_MASK);
        long bytesPerPage = MemoryUsage.sizeOfByteArray(BumpAllocator.PAGE_SIZE);
        return MemoryRange.of(pages * bytesPerPage + MemoryUsage.sizeOfObjectArray(pages));
    }

    private long[][] pages;
    private HugeIntArray degrees;
    private HugeLongArray offsets;

    public UncompressedAdjacencyList(long[][] pages, HugeIntArray degrees, HugeLongArray offsets) {
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
        var cursor = new Cursor(pages);
        var offset = offsets.get(node);
        cursor.init(offset, degree);
        return cursor;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        var degree = degrees.get(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }
        if (reuse instanceof Cursor) {
            reuse.init(offsets.get(node), degree);
            return reuse;
        }
        return adjacencyCursor(node, fallbackValue);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return new Cursor(pages);
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
        var degree = degrees.get(node);
        if (degree == 0) {
            return PropertyCursor.empty();
        }
        if (reuse instanceof Cursor) {
            reuse.init(offsets.get(node), degree);
            return reuse;
        }
        return propertyCursor(node, fallbackValue);
    }

    @Override
    public PropertyCursor rawPropertyCursor() {
        return new Cursor(pages);
    }

    public static final class Cursor extends MutableIntValue implements AdjacencyCursor, PropertyCursor {

        private long[][] pages;

        private long[] currentPage;
        private int degree;
        private int limit;
        private int offset;

        private Cursor(long[][] pages) {
            this.pages = pages;
        }

        @Override
        public void init(long fromIndex, int degree) {
            currentPage = pages[pageIndex(fromIndex, BumpAllocator.PAGE_SHIFT)];
            offset = indexInPage(fromIndex, BumpAllocator.PAGE_MASK);
            limit = offset + degree;
            this.degree = degree;
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
            var dest = destination instanceof Cursor
                ? (Cursor) destination
                : new Cursor(pages);

            dest.currentPage = currentPage;

            dest.degree = degree;
            dest.limit = limit;
            dest.offset = offset;

            return dest;
        }

        @Override
        public long skipUntil(long target) {
            if (remaining() <= 0) {
                return NOT_FOUND;
            }

            var idx = ArrayUtil.binarySearchLast(currentPage, offset, limit, target);

            long value;

            if (idx >= 0) { // Found
                offset = idx;

                // We need to skip the current offset.
                if (offset + 1 < limit) { // Page has more elements.
                    value = currentPage[offset + 1];
                    offset = offset + 2;
                } else { // Target is the last element.
                    value = currentPage[offset++];
                }
            } else { // Not found
                offset = -idx - 1; // Index of next largest element.
                if (offset < limit) { // Page has more elements.
                    value = currentPage[offset++];
                } else {
                    value = currentPage[offset - 1];
                }
            }

            return value;
        }

        @Override
        public long advance(long target) {
            if (remaining() <= 0) {
                return NOT_FOUND;
            }

            var idx = ArrayUtil.binarySearchFirst(currentPage, offset, limit, target);

            if (idx >= 0) { // Found
                offset = idx;
            } else {
                // Set the offset to the element that is greater than target.
                offset = -idx - 1;

                if (offset == 0 || offset >= limit) { // Target is out of range
                    offset = limit;
                    return currentPage[offset - 1];
                }
            }

            return currentPage[offset++];
        }

        @Override
        public long advanceBy(int n) {
            assert n >= 0;

            offset += n;
            if (offset >= limit) {
                offset = limit;
                return NOT_FOUND;
            }
            return currentPage[offset];
        }

        @Override
        public void close() {
            pages = null;
            currentPage = null;
        }
    }
}
