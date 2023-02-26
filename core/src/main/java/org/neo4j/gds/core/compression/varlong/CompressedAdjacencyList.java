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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.loading.MutableIntValue;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;

import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.collections.PageUtil.indexInPage;
import static org.neo4j.gds.collections.PageUtil.pageIndex;
import static org.neo4j.gds.core.compression.common.VarLongEncoding.encodedVLongSize;
import static org.neo4j.gds.mem.BitUtil.ceilDiv;

public final class CompressedAdjacencyList implements AdjacencyList {

    public static MemoryEstimation adjacencyListEstimation(RelationshipType relationshipType, boolean undirected) {
        return MemoryEstimations.setup("", dimensions -> {
            long nodeCount = dimensions.nodeCount();
            long relCountForType = dimensions
                .relationshipCounts()
                .getOrDefault(relationshipType, dimensions.relCountUpperBound());
            long relCount = undirected ? relCountForType * 2 : relCountForType;
            long avgDegree = (nodeCount > 0) ? ceilDiv(relCount, nodeCount) : 0L;
            return CompressedAdjacencyList.adjacencyListEstimation(avgDegree, nodeCount);
        });
    }

    public static MemoryEstimation adjacencyListEstimation(long avgDegree, long nodeCount) {
        // Best case scenario:
        // Difference between node identifiers in each adjacency list is 1.
        // This leads to ideal compression through delta encoding.
        int deltaBestCase = 1;
        long bestCaseAdjacencySize = computeAdjacencyByteSize(avgDegree, nodeCount, deltaBestCase);

        // Worst case scenario:
        // Relationships are equally distributed across nodes, i.e. each node has the same number of rels.
        // Within each adjacency list, all identifiers have the highest possible difference between each other.
        // Highest possible difference is the number of nodes divided by the average degree.
        long deltaWorstCase = (avgDegree > 0) ? ceilDiv(nodeCount, avgDegree) : 0L;
        long worstCaseAdjacencySize = computeAdjacencyByteSize(avgDegree, nodeCount, deltaWorstCase);

        int minPages = PageUtil.numPagesFor(bestCaseAdjacencySize, BumpAllocator.PAGE_SHIFT, BumpAllocator.PAGE_MASK);
        int maxPages = PageUtil.numPagesFor(worstCaseAdjacencySize, BumpAllocator.PAGE_SHIFT, BumpAllocator.PAGE_MASK);

        long bytesPerPage = MemoryUsage.sizeOfByteArray(BumpAllocator.PAGE_SIZE);
        long minMemoryReqs = minPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(minPages);
        long maxMemoryReqs = maxPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(maxPages);

        MemoryRange pagesMemoryRange = MemoryRange.of(minMemoryReqs, maxMemoryReqs);

        return MemoryEstimations
            .builder(CompressedAdjacencyList.class)
            .fixed("pages", pagesMemoryRange)
            .perNode("degrees", HugeIntArray::memoryEstimation)
            .perNode("offsets", HugeLongArray::memoryEstimation)
            .build();
    }

    @TestOnly
    public static MemoryEstimation adjacencyListEstimation(boolean undirected) {
        return adjacencyListEstimation(ALL_RELATIONSHIPS, undirected);
    }

    public static long computeAdjacencyByteSize(long avgDegree, long nodeCount, long delta) {
        long firstAdjacencyIdAvgByteSize = (avgDegree > 0) ? ceilDiv(encodedVLongSize(nodeCount), 2) : 0L;
        int relationshipByteSize = encodedVLongSize(delta);
        long compressedAdjacencyByteSize = relationshipByteSize * Math.max(0, (avgDegree - 1));
        return (firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize) * nodeCount;
    }

    private byte[][] pages;
    private HugeIntArray degrees;
    private HugeLongArray offsets;

    public CompressedAdjacencyList(byte[][] pages, HugeIntArray degrees, HugeLongArray offsets) {
        this.pages = pages;
        this.degrees = degrees;
        this.offsets = offsets;
    }

    @Override
    public int degree(long node) {
        return degrees.get(node);
    }

    // Cursors

    @Override
    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        var degree = degrees.get(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }
        var cursor = new DecompressingCursor(pages);
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
        if (reuse instanceof DecompressingCursor) {
            reuse.init(offsets.get(node), degree);
            return reuse;
        }
        return adjacencyCursor(node, fallbackValue);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return new DecompressingCursor(pages);
    }

    public static final class DecompressingCursor extends MutableIntValue implements AdjacencyCursor {

        private byte[][] pages;
        private final AdjacencyDecompressingReader decompress;

        private int maxTargets;
        private int currentPosition;

        private DecompressingCursor(byte[][] pages) {
            this.pages = pages;
            this.decompress = new AdjacencyDecompressingReader();
        }

        @Override
        public void init(long fromIndex, int degree) {
            maxTargets = decompress.reset(
                pages[pageIndex(fromIndex, BumpAllocator.PAGE_SHIFT)],
                indexInPage(fromIndex, BumpAllocator.PAGE_MASK),
                degree
            );
            currentPosition = 0;
        }

        @Override
        public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
            var dest = destination instanceof DecompressingCursor
                ? (DecompressingCursor) destination
                : new DecompressingCursor(pages);
            dest.decompress.copyFrom(this.decompress);
            dest.currentPosition = this.currentPosition;
            dest.maxTargets = this.maxTargets;
            return dest;
        }

        @Override
        public int size() {
            return maxTargets;
        }

        @Override
        public int remaining() {
            return maxTargets - currentPosition;
        }

        @Override
        public boolean hasNextVLong() {
            return currentPosition < maxTargets;
        }

        @Override
        public long nextVLong() {
            int current = currentPosition++;
            int remaining = maxTargets - current;
            return decompress.next(remaining);
        }

        @Override
        public long peekVLong() {
            int remaining = maxTargets - currentPosition;
            return decompress.peek(remaining);
        }

        /**
         * Read and decode target ids until it is strictly larger than ({@literal >}) the provided {@code target}.
         * If there are no such targets before this cursor is exhausted, {@link org.neo4j.gds.api.AdjacencyCursor#NOT_FOUND -1} is returned.
         */
        @Override
        public long skipUntil(long target) {
            long value = decompress.skipUntil(target, remaining(), this);
            this.currentPosition += this.value;
            return value;
        }

        /**
         * Read and decode target ids until it is larger than or equal ({@literal >=}) the provided {@code target}.
         * If there are no such targets before this cursor is exhausted, {@link org.neo4j.gds.api.AdjacencyCursor#NOT_FOUND -1} is returned.
         */
        @Override
        public long advance(long target) {
            int targetsLeftToBeDecoded = remaining();
            if (targetsLeftToBeDecoded <= 0) {
                return AdjacencyCursor.NOT_FOUND;
            }
            long value = decompress.advance(target, targetsLeftToBeDecoded, this);
            this.currentPosition += this.value;
            return value;
        }

        @Override
        public long advanceBy(int n) {
            assert n >= 0;

            int targetsLeftToBeDecoded = remaining();
            if (targetsLeftToBeDecoded <= n) {
                return AdjacencyCursor.NOT_FOUND;
            }

            var value = decompress.advanceBy(n, targetsLeftToBeDecoded, this);
            this.currentPosition += this.value;
            return value;
        }
    }
}
