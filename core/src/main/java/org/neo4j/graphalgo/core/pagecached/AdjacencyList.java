/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.pagecached;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.core.loading.MutableIntValue;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.core.loading.VarLongEncoding.encodedVLongSize;
import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public final class AdjacencyList {

    public static final int PAGE_SHIFT = 18;
    public static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    public static final long PAGE_MASK = PAGE_SIZE - 1;

    //    private final long allocatedMemory;
    private final PagedFile pagedFile;
//    private byte[][] pages;

    public static MemoryEstimation compressedMemoryEstimation(long avgDegree, long nodeCount) {
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

        int minPages = PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        int maxPages = PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);

        long bytesPerPage = MemoryUsage.sizeOfByteArray(PAGE_SIZE);
        long minMemoryReqs = minPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(minPages);
        long maxMemoryReqs = maxPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(maxPages);

        MemoryRange pagesMemoryRange = MemoryRange.of(minMemoryReqs, maxMemoryReqs);

        return MemoryEstimations
            .builder(AdjacencyList.class)
            .fixed("pages", pagesMemoryRange)
            .build();
    }

    public static MemoryEstimation compressedMemoryEstimation(boolean undirected) {
        return compressedMemoryEstimation(ALL_RELATIONSHIPS, undirected);
    }

    public static MemoryEstimation compressedMemoryEstimation(RelationshipType relationshipType, boolean undirected) {
        return MemoryEstimations.setup("", dimensions -> {
            long nodeCount = dimensions.nodeCount();
            long relCountForType = dimensions
                .relationshipCounts()
                .getOrDefault(relationshipType, dimensions.maxRelCount());
            long relCount = undirected ? relCountForType * 2 : relCountForType;
            long avgDegree = (nodeCount > 0) ? ceilDiv(relCount, nodeCount) : 0L;
            return AdjacencyList.compressedMemoryEstimation(avgDegree, nodeCount);
        });
    }

    public static MemoryEstimation uncompressedMemoryEstimation(boolean undirected) {
        return uncompressedMemoryEstimation(ALL_RELATIONSHIPS, undirected);
    }

    public static MemoryEstimation uncompressedMemoryEstimation(RelationshipType relationshipType, boolean undirected) {

        return MemoryEstimations
            .builder(AdjacencyList.class)
            .perGraphDimension("pages", (dimensions, concurrency) -> {
                long nodeCount = dimensions.nodeCount();
                long relCountForType = dimensions
                    .relationshipCounts()
                    .getOrDefault(relationshipType, dimensions.maxRelCount());
                long relCount = undirected ? relCountForType * 2 : relCountForType;

                long uncompressedAdjacencySize = relCount * Long.BYTES + nodeCount * Integer.BYTES;
                int pages = PageUtil.numPagesFor(uncompressedAdjacencySize, PAGE_SHIFT, PAGE_MASK);
                long bytesPerPage = MemoryUsage.sizeOfByteArray(PAGE_SIZE);

                return MemoryRange.of(pages * bytesPerPage + MemoryUsage.sizeOfObjectArray(pages));
            })
            .build();
    }

    /* test private */
    static long computeAdjacencyByteSize(long avgDegree, long nodeCount, long delta) {
        long firstAdjacencyIdAvgByteSize = (avgDegree > 0) ? ceilDiv(encodedVLongSize(nodeCount), 2) : 0L;
        int relationshipByteSize = encodedVLongSize(delta);
        int degreeByteSize = Integer.BYTES;
        long compressedAdjacencyByteSize = relationshipByteSize * Math.max(0, (avgDegree - 1));
        return (degreeByteSize + firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize) * nodeCount;
    }

    public AdjacencyList(PagedFile pagedFile) {
        this.pagedFile = pagedFile;
//        this.allocatedMemory = memoryOfPages(pages);
    }

//    private static long memoryOfPages(byte[][] pages) {
//        long memory = MemoryUsage.sizeOfObjectArray(pages.length);
//        for (byte[] page : pages) {
//            if (page != null) {
//                memory += MemoryUsage.sizeOfByteArray(page.length);
//            }
//        }
//        return memory;
//    }

//    int getDegree(long index) {
//        return AdjacencyDecompressingReader.readInt(
//                pages[pageIndex(index, PAGE_SHIFT)],
//                indexInPage(index, PAGE_MASK));
//    }

    public final long release() {
        pagedFile.close();
//        if (pages == null) {
        return 0L;
//        }
//        pages = null;
//        return allocatedMemory;
    }

    // Cursors

    Cursor cursor(long offset) throws IOException {
        PageCursor pageCursor = pagedFile.io(
            0,
            PagedFile.PF_SHARED_READ_LOCK,
            PageCursorTracer.NULL
        );
        return new Cursor(pageCursor).init(offset, pagedFile.pageSize());
    }

    /**
     * Returns a new, uninitialized delta cursor. Call {@link DecompressingCursor#init(long, int)}.
     */
    DecompressingCursor rawDecompressingCursor() throws IOException {
        PageCursor pageCursor = pagedFile.io(
            0,
            PagedFile.PF_SHARED_READ_LOCK,
            PageCursorTracer.NULL
        );
        return new DecompressingCursor(pageCursor);
    }

    /**
     * Get a new cursor initialised on the given offset
     */
    DecompressingCursor decompressingCursor(long offset) throws IOException {
        return rawDecompressingCursor().init(offset, pagedFile.pageSize());
    }

    /**
     * Initialise the given cursor with the given offset
     */
    DecompressingCursor decompressingCursor(DecompressingCursor reuse, long offset) throws IOException {
        return reuse.init(offset, pagedFile.pageSize());
    }

    public static final class Cursor extends MutableIntValue {

//        static final Cursor EMPTY = new Cursor(new byte[0][]);

        // TODO: free
        private final PageCursor pageCursor;

        //        private byte[] currentPage;
        private int degree;
        private int offset;
        private int limit;

        private Cursor(PageCursor pageCursor) {
            this.pageCursor = pageCursor;
        }

        public int length() {
            return degree;
        }

        /**
         * Return true iff there is at least one more target to decode.
         */
        boolean hasNextLong() {
            return offset < limit;
        }

        /**
         * Read the next target id.
         * It is undefined behavior if this is called after {@link #hasNextLong()} returns {@code false}.
         */
        long nextLong() throws IOException {
            if (pageCursor.getCurrentPageSize() - offset >= Long.BYTES) {
                long value = pageCursor.getLong(offset);
                offset += Long.BYTES;
                return value;
            }

            byte[] partialLong = new byte[Long.BYTES];
            int bytesOnThisPage = pageCursor.getCurrentPageSize() - offset;
            pageCursor.setOffset(offset);
            pageCursor.getBytes(partialLong, 0, bytesOnThisPage);

            return getLong(partialLong, bytesOnThisPage);
        }

        Cursor init(long offset, int pageSize) throws IOException {
            this.offset = (int) (offset % pageSize);
            pageCursor.next(offset / pageSize);
//            this.currentPage = pages[pageIndex(fromIndex, PAGE_SHIFT)];
//            this.offset = indexInPage(fromIndex, PAGE_MASK);
            this.degree = pageCursor.getInt(this.offset);
            this.offset += Integer.BYTES;
            this.limit = this.offset + degree * Long.BYTES;
            return this;
        }

        private long getLong(byte[] longBytes, int alreadyRead) throws IOException {
            if (!pageCursor.next()) {
                return -1;
            }

            pageCursor.getBytes(longBytes, alreadyRead, Long.BYTES - alreadyRead);
            this.offset = pageCursor.getOffset();
            return ByteBuffer.wrap(longBytes).order(ByteOrder.BIG_ENDIAN).getLong();
        }
    }

    public static final class DecompressingCursor extends MutableIntValue {

        public static final long NOT_FOUND = -1;
        // TODO: free
        private byte[][] pages;
        private final AdjacencyDecompressingReader decompress;

        private int maxTargets;
        private int currentTarget;
        private PageCursor pageCursor;

        private DecompressingCursor(PageCursor pageCursor) {
            this.pageCursor = pageCursor;
            this.decompress = new AdjacencyDecompressingReader();
        }

        /**
         * Copy iteration state from another cursor without changing {@code other}.
         */
        void copyFrom(DecompressingCursor other) {
            decompress.copyFrom(other.decompress);
            currentTarget = other.currentTarget;
            maxTargets = other.maxTargets;
        }

        /**
         * Return how many targets can be decoded in total. This is equivalent to the degree.
         */
        public int length() {
            return maxTargets;
        }

        /**
         * Return how many targets are still left to be decoded.
         */
        int remaining() {
            return maxTargets - currentTarget;
        }

        /**
         * Return true iff there is at least one more target to decode.
         */
        boolean hasNextVLong() {
            return currentTarget < maxTargets;
        }

        /**
         * Read and decode the next target id.
         * It is undefined behavior if this is called after {@link #hasNextVLong()} returns {@code false}.
         */
        long nextVLong() throws IOException {
            int current = currentTarget++;
            int remaining = maxTargets - current;
            return decompress.next(remaining);
        }

        /**
         * Read and decode target ids until it is strictly larger than (`>`) the provided {@code target}.
         * Might return an id that is less than or equal to {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code skipUntil(target) <= target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        long skipUntil(long target) throws IOException {
            long value = decompress.skipUntil(target, remaining(), this);
            this.currentTarget += this.value;
            return value;
        }

        /**
         * Read and decode target ids until it is larger than or equal (`>=`) the provided {@code target}.
         * Might return an id that is less than {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code advance(target) < target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        long advance(long target) throws IOException {
            int targetsLeftToBeDecoded = remaining();
            if (targetsLeftToBeDecoded <= 0) {
                return NOT_FOUND;
            }
            long value = decompress.advance(target, targetsLeftToBeDecoded, this);
            this.currentTarget += this.value;
            return value;
        }

        DecompressingCursor init(long fromIndex, int pageSize) throws IOException {
            pageCursor.next(fromIndex / pageSize);
            maxTargets = decompress.reset(pageCursor, (int) (fromIndex % pageSize));
            currentTarget = 0;
            return this;
        }
    }
}
