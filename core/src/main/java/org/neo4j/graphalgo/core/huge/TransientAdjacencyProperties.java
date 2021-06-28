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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.RelationshipType;
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

public final class TransientAdjacencyProperties implements AdjacencyProperties {

    public static MemoryEstimation uncompressedMemoryEstimation(RelationshipType relationshipType, boolean undirected) {
        return MemoryEstimations
            .builder(TransientAdjacencyProperties.class)
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

    public TransientAdjacencyProperties(long[][] pages, HugeIntArray degrees, HugeLongArray offsets) {
        this.pages = pages;
        this.degrees = degrees;
        this.offsets = offsets;
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

    private static final class Cursor extends MutableIntValue implements PropertyCursor {

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
}
