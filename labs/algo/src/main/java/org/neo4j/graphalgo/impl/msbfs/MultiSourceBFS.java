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
package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Multi Source Breadth First Search implemented as described in [1].
 * <p>
 * The benefit of running this MS-BFS instead of multiple execution of a regular
 * BFS for every source is that the MS-BFS algorithm can collapse traversals that are
 * the same for multiple nodes. If any two or more given BFSs would traverse the same nodes
 * at the same iteration depth, the MS-BFS will traverse only once and group all sources
 * for this traversal.
 * <p>
 * The consumer of this algorithm provides a callback function, the gets called
 * with:
 * <ul>
 * <li>the node id where the BFS traversal is at</li>
 * <li>the depth or BFS iteration at which this node is traversed</li>
 * <li>a lazily evaluated list of all source nodes that have arrived at this node at the same depth/iteration</li>
 * </ul>
 * The sources iterator is only valid during the execution of the callback and
 * should not be stored.
 * <p>
 * We use a fixed {@code ω} (OMEGA) of 32, which allows us to implement the
 * seen/visitNext bit sets as a packed long which improves memory locality
 * as suggested in 4.1. of the paper.
 * If the number of sources exceed 32, multiple instances of MS-BFS are run
 * in parallel.
 * <p>
 * If the MS-BFS runs in parallel, the callback may be executed from multiple threads
 * at the same time. The implementation should therefore be thread-safe.
 * <p>
 * The algorithm provides two invariants:
 * <ul>
 * <li>
 * For a single thread, a single node is traversed at most once at a given depth
 * – That is, the combination of {@code (nodeId, depth)} appears at most once per thread.
 * It may be that a node is traversed multiple times, but then always at different depths.
 * </li>
 * <li>
 * For multiple threads, the {@code (nodeId, depth)} may appear multiple times, but then always
 * for different sources.
 * </li>
 * </ul>
 * <p>
 * [1]: <a href="http://www.vldb.org/pvldb/vol8/p449-then.pdf">The More the Merrier: Efficient Multi-Source Graph Traversal</a>
 */
public final class MultiSourceBFS implements Runnable {

    // how many sources can be traversed simultaneously
    static final int OMEGA = 64;

    private final ThreadLocal<HugeLongArray> visits;
    private final ThreadLocal<HugeLongArray> nexts;
    private final ThreadLocal<HugeLongArray> seens;

    private final IdMapping nodeIds;
    private final RelationshipIterator relationships;
    private final Direction direction;
    private final BfsConsumer perNodeAction;
    private final long[] startNodes;
    private int sourceNodeCount;
    private long nodeOffset;
    private long nodeCount;

    public MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            AllocationTracker tracker,
            long... startNodes) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = (startNodes != null && startNodes.length > 0) ? startNodes : null;
        if (this.startNodes != null) {
            Arrays.sort(this.startNodes);
        }
        nodeCount = nodeIds.nodeCount();
        this.visits = new LocalHugeLongArray(nodeCount, tracker);
        this.nexts = new LocalHugeLongArray(nodeCount, tracker);
        this.seens = new LocalHugeLongArray(nodeCount, tracker);
    }

    private MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            long nodeCount,
            ThreadLocal<HugeLongArray> visits,
            ThreadLocal<HugeLongArray> nexts,
            ThreadLocal<HugeLongArray> seens,
            long... startNodes) {
        assert startNodes != null && startNodes.length > 0;
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = startNodes;
        this.nodeCount = nodeCount;
        this.visits = visits;
        this.nexts = nexts;
        this.seens = seens;
    }

    private MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            long nodeCount,
            long nodeOffset,
            int sourceNodeCount,
            ThreadLocal<HugeLongArray> visits,
            ThreadLocal<HugeLongArray> nexts,
            ThreadLocal<HugeLongArray> seens) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = null;
        this.nodeCount = nodeCount;
        this.nodeOffset = nodeOffset;
        this.sourceNodeCount = sourceNodeCount;
        this.visits = visits;
        this.nexts = nexts;
        this.seens = seens;
    }

    /**
     * Runs MS-BFS, possibly in parallel.
     */
    public void run(int concurrency, ExecutorService executor) {
        final int threads = numberOfThreads();
        Collection<MultiSourceBFS> bfss = allSourceBfss(threads);
        if (!ParallelUtil.canRunInParallel(executor)) {
            // fallback to sequentially running all MS-BFS instances
            executor = null;
        }
        ParallelUtil.runWithConcurrency(
                concurrency,
                bfss,
                threads << 2,
                100L,
                TimeUnit.MICROSECONDS,
                executor);
    }

    /**
     * Runs MS-BFS, always single-threaded. Requires that there are at most
     * 32 startNodes. If there are more, {@link #run(int, ExecutorService)} must be used.
     */
    @Override
    public void run() {
        assert sourceLength() <= OMEGA : "more than " + OMEGA + " sources not supported";

        long totalNodeCount = this.nodeCount;

        HugeLongArray visitSet = visits.get();
        HugeLongArray nextSet = nexts.get();
        HugeLongArray seenSet = seens.get();

        final SourceNodes sourceNodes;
        if (startNodes == null) {
            sourceNodes = prepareOffsetSources(visitSet, seenSet);
        } else {
            sourceNodes = prepareSpecifiedSources(visitSet, seenSet);
        }

        runLocalMsbfs(totalNodeCount, sourceNodes, visitSet, nextSet, seenSet);
    }

    private SourceNodes prepareOffsetSources(HugeLongArray visitSet, HugeLongArray seenSet) {
        int localNodeCount = this.sourceNodeCount;
        long nodeOffset = this.nodeOffset;
        SourceNodes sourceNodes = new SourceNodes(nodeOffset, localNodeCount);

        for (int i = 0; i < localNodeCount; ++i) {
            seenSet.set(nodeOffset + i, 1L << i);
            visitSet.or(nodeOffset + i, 1L << i);
        }

        return sourceNodes;
    }

    private SourceNodes prepareSpecifiedSources(HugeLongArray visitSet, HugeLongArray seenSet) {
        assert isSorted(startNodes);

        long[] startNodes = this.startNodes;
        int localNodeCount = startNodes.length;
        SourceNodes sourceNodes = new SourceNodes(startNodes);

        for (int i = 0; i < localNodeCount; ++i) {
            long nodeId = startNodes[i];
            seenSet.set(nodeId, 1L << i);
            visitSet.or(nodeId, 1L << i);
        }

        return sourceNodes;
    }

    private void runLocalMsbfs(
            long totalNodeCount,
            SourceNodes sourceNodes,
            HugeLongArray visitSet,
            HugeLongArray nextSet,
            HugeLongArray seenSet) {

        HugeCursor<long[]> visitCursor = visitSet.newCursor();
        HugeCursor<long[]> nextCursor = nextSet.newCursor();
        int depth = 0;

        while (true) {
            visitSet.initCursor(visitCursor);
            while (visitCursor.next()) {
                long[] array = visitCursor.array;
                int offset = visitCursor.offset;
                int limit = visitCursor.limit;
                long base = visitCursor.base;
                for (int i = offset; i < limit; ++i) {
                    if (array[i] != 0L) {
                        prepareNextVisit(array[i], base + i, nextSet);
                    }
                }
            }


            ++depth;

            boolean hasNext = false;
            long next;

            nextSet.initCursor(nextCursor);
            while (nextCursor.next()) {
                long[] array = nextCursor.array;
                int offset = nextCursor.offset;
                int limit = nextCursor.limit;
                long base = nextCursor.base;
                for (int i = offset; i < limit; ++i) {
                    if (array[i] != 0L) {
                        next = visitNext(base + i, seenSet, nextSet);
                        if (next != 0L) {
                            sourceNodes.reset(next);
                            perNodeAction.accept(base + i, depth, sourceNodes);
                            hasNext = true;
                        }
                    }
                }
            }

            if (!hasNext) {
                return;
            }

            nextSet.copyTo(visitSet, totalNodeCount);
            nextSet.fill(0L);
        }
    }

    private void prepareNextVisit(long nodeVisit, long nodeId, HugeLongArray nextSet) {
        relationships.forEachRelationship(
                nodeId,
                direction,
                (src, tgt) -> {
                    nextSet.or(tgt, nodeVisit);
                    return true;
                });
    }

    private long visitNext(long nodeId, HugeLongArray seenSet, HugeLongArray nextSet) {
        long seen = seenSet.get(nodeId);
        long next = nextSet.and(nodeId, ~seen);
        seenSet.or(nodeId, next);
        return next;
    }

    /* assert-only */ private boolean isSorted(long[] nodes) {
        long[] copy = Arrays.copyOf(nodes, nodes.length);
        Arrays.sort(copy);
        return Arrays.equals(copy, nodes);
    }

    private long sourceLength() {
        if (startNodes != null) {
            return startNodes.length;
        }
        if (sourceNodeCount == 0) {
            return nodeCount;
        }
        return sourceNodeCount;
    }

    private int numberOfThreads() {
        long sourceLength = sourceLength();
        long threads = ParallelUtil.threadCount(OMEGA, sourceLength);
        if ((int) threads != threads) {
            throw new IllegalArgumentException("Unable run MS-BFS on " + sourceLength + " sources.");
        }
        return (int) threads;
    }

    // lazily creates MS-BFS instances for OMEGA sized source chunks
    private Collection<MultiSourceBFS> allSourceBfss(int threads) {
        if (startNodes == null) {
            long sourceLength = nodeCount;
            return new ParallelMultiSources(threads, sourceLength) {
                @Override
                MultiSourceBFS next(final long from, final int length) {
                    return new MultiSourceBFS(
                            nodeIds,
                            relationships.concurrentCopy(),
                            direction,
                            perNodeAction,
                            sourceLength,
                            from,
                            length,
                            visits,
                            nexts,
                            seens
                    );
                }
            };
        }
        long[] startNodes = this.startNodes;
        int sourceLength = startNodes.length;
        return new ParallelMultiSources(threads, sourceLength) {
            @Override
            MultiSourceBFS next(final long from, final int length) {
                return new MultiSourceBFS(
                        nodeIds,
                        relationships.concurrentCopy(),
                        direction,
                        perNodeAction,
                        nodeCount,
                        visits,
                        nexts,
                        seens,
                        Arrays.copyOfRange(startNodes, (int) from, (int) (from + length))
                );
            }
        };
    }

    @Override
    public String toString() {
        if (startNodes != null && startNodes.length > 0) {
            return "MSBFS{" + startNodes[0] +
                    " .. " + (startNodes[startNodes.length - 1] + 1) +
                    " (" + startNodes.length +
                    ")}";
        }
        return "MSBFS{" + nodeOffset +
                " .. " + (nodeOffset + sourceNodeCount) +
                " (" + sourceNodeCount +
                ")}";
    }

    private static final class SourceNodes implements BfsSources {
        private final long[] sourceNodes;
        private final int maxPos;
        private final int startPos;
        private final long offset;
        private long sourceMask;
        private int pos;

        private SourceNodes(long[] sourceNodes) {
            assert sourceNodes.length <= OMEGA;
            this.sourceNodes = sourceNodes;
            this.maxPos = sourceNodes.length;
            this.offset = 0L;
            this.startPos = -1;
        }

        private SourceNodes(long offset, int length) {
            assert length <= OMEGA;
            this.sourceNodes = null;
            this.maxPos = length;
            this.offset = offset;
            this.startPos = -1;
        }

        public void reset() {
            this.pos = startPos;
            fetchNext();
        }

        void reset(long sourceMask) {
            this.sourceMask = sourceMask;
            reset();
        }

        @Override
        public boolean hasNext() {
            return pos < maxPos;
        }

        @Override
        public long next() {
            int current = this.pos;
            fetchNext();
            return sourceNodes != null ? sourceNodes[current] : (long) current + offset;
        }

        @Override
        public int size() {
            return Long.bitCount(sourceMask);
        }

        private void fetchNext() {
            //noinspection StatementWithEmptyBody
            while (++pos < maxPos && (sourceMask & (1L << pos)) == 0L)
                ;
        }
    }

    private static abstract class ParallelMultiSources extends AbstractCollection<MultiSourceBFS> implements Iterator<MultiSourceBFS> {
        private final int threads;
        private final long sourceLength;
        private long start = 0L;
        private int i = 0;

        private ParallelMultiSources(int threads, long sourceLength) {
            this.threads = threads;
            this.sourceLength = sourceLength;
        }

        @Override
        public boolean hasNext() {
            return i < threads;
        }

        @Override
        public int size() {
            return threads;
        }

        @Override
        public Iterator<MultiSourceBFS> iterator() {
            start = 0L;
            i = 0;
            return this;
        }

        @Override
        public MultiSourceBFS next() {
            int len = (int) Math.min(OMEGA, sourceLength - start);
            MultiSourceBFS bfs = next(start, len);
            start += len;
            i++;
            return bfs;
        }

        abstract MultiSourceBFS next(long from, int length);
    }

    private static final class LocalHugeLongArray extends ThreadLocal<HugeLongArray> {
        private final long size;
        private final AllocationTracker tracker;

        private LocalHugeLongArray(final long size, final AllocationTracker tracker) {
            this.size = size;
            this.tracker = tracker;
        }

        @Override
        protected HugeLongArray initialValue() {
            return HugeLongArray.newArray(size, tracker);
        }

        @Override
        public HugeLongArray get() {
            HugeLongArray values = super.get();
            values.fill(0L);
            return values;
        }
    }
}
