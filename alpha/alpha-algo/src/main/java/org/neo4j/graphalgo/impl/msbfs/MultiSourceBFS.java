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
package org.neo4j.graphalgo.impl.msbfs;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.MultiPartiteRelationshipIterator;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.utils.CloseableThreadLocal;

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
 * We use a fixed {@code ω} (OMEGA) of 64, which allows us to implement the
 * seen/visitNext bit sets as a packed long which improves memory locality
 * as suggested in 4.1. of the paper.
 * If the number of sources exceed 64, multiple instances of MS-BFS are run
 * in parallel.
 * <p>
 * If the MS-BFS runs in parallel, the callback may be executed from multiple threads
 * at the same time. The implementation should therefore be thread-safe.
 * <p>
 * [1]: <a href="http://www.vldb.org/pvldb/vol8/p449-then.pdf">The More the Merrier: Efficient Multi-Source Graph Traversal</a>
 */
public final class MultiSourceBFS implements Runnable {

    interface ExecutionStrategy {

        void run(
            MultiPartiteRelationshipIterator relationships,
            long totalNodeCount,
            SourceNodes sourceNodes,
            HugeLongArray visitSet,
            HugeLongArray visitNextSet,
            HugeLongArray seenSet,
            @Nullable HugeLongArray seenNextSet
        );
    }

    // how many sources can be traversed simultaneously
    public static final int OMEGA = 64;

    private final CloseableThreadLocal<HugeLongArray> visits;
    private final CloseableThreadLocal<HugeLongArray> visitsNext;
    private final CloseableThreadLocal<HugeLongArray> seens;
    private final CloseableThreadLocal<HugeLongArray> seensNext;

    private final long nodeCount;
    private final IdMapping nodeIds;
    private final MultiPartiteRelationshipIterator relationships;
    private final ExecutionStrategy strategy;
    private final boolean allowStartNodeTraversal;
    private final long[] startNodes;
    private int sourceNodeCount;
    private long nodeOffset;

    /**
     * Initializes MS-BFS prepared for executing the Aggregated Neighbor Processing strategy.
     * <p>
     * The created MS-BFS can be shared between multiple threads.
     * This method is supposed to be used if the MS-BFS needs to be executed
     * with different consumers, e.g. for reusing not-thread-safe consumers.
     * <p>
     * To finalize initialization, one must call:
     * {@link MultiSourceBFS#initAggregatedNeighborProcessing}.
     */
    public static MultiSourceBFS aggregatedNeighborProcessing(Graph graph, AllocationTracker tracker) {
        return new MultiSourceBFS(graph, graph, null, false, false, tracker);
    }

    public static MultiSourceBFS aggregatedNeighborProcessing(
        IdMapping nodeIds,
        MultiPartiteRelationshipIterator relationships,
        BfsConsumer perNodeAction,
        AllocationTracker tracker,
        long... startNodes
    ) {
        return new MultiSourceBFS(nodeIds, relationships, new ANPStrategy(perNodeAction), false, false, tracker, startNodes);
    }

    /**
     * Initializes MS-BFS prepared for executing the Predecessor Processing strategy.
     * <p>
     * The created MS-BFS can be shared between multiple threads and is supposed to
     * be used if the MS-BFS needs to be executed with different consumers,
     * e.g. for reusing not-thread-safe consumers.
     * <p>
     * To finalize initialization, one must call:
     * {@link MultiSourceBFS#initPredecessorProcessing}.
     */
    public static MultiSourceBFS predecessorProcessing(Graph graph, AllocationTracker tracker) {
        return new MultiSourceBFS(graph, graph, null, true, false, tracker);
    }

    public static MultiSourceBFS predecessorProcessing(
        Graph graph,
        BfsConsumer perNodeAction,
        BfsWithPredecessorConsumer perNeighborAction,
        AllocationTracker tracker,
        long... startNodes
    ) {
        return new MultiSourceBFS(
            graph,
            graph,
            new PredecessorStrategy(perNodeAction, perNeighborAction),
            true,
            false,
            tracker,
            startNodes
        );
    }

    public MultiSourceBFS initAggregatedNeighborProcessing(BfsConsumer perNodeAction, long[] startNodes) {
        return new MultiSourceBFS(
            nodeIds,
            relationships.concurrentCopy(),
            new ANPStrategy(perNodeAction),
            nodeCount,
            false,
            visits,
            visitsNext,
            seens,
            seensNext,
            startNodes
        );
    }

    public MultiSourceBFS initPredecessorProcessing(
        BfsConsumer perNodeAction,
        BfsWithPredecessorConsumer perNeighborAction,
        long[] startNodes
    ) {
        return new MultiSourceBFS(
            nodeIds,
            relationships.concurrentCopy(),
            new PredecessorStrategy(perNodeAction, perNeighborAction),
            nodeCount,
            false,
            visits,
            visitsNext,
            seens,
            seensNext,
            startNodes
        );
    }

    public MultiSourceBFS(
        IdMapping nodeIds,
        MultiPartiteRelationshipIterator relationships,
        ExecutionStrategy strategy,
        boolean initSeenNext,
        boolean allowStartNodeTraversal,
        AllocationTracker tracker,
        long... startNodes
    ) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.strategy = strategy;
        this.allowStartNodeTraversal = allowStartNodeTraversal;
        this.startNodes = (startNodes != null && startNodes.length > 0) ? startNodes : null;
        if (this.startNodes != null) {
            Arrays.sort(this.startNodes);
        }
        this.nodeCount = nodeIds.nodeCount();
        this.visits = new LocalHugeLongArray(nodeCount, tracker);
        this.visitsNext = new LocalHugeLongArray(nodeCount, tracker);
        this.seens = new LocalHugeLongArray(nodeCount, tracker);
        this.seensNext = initSeenNext ? new LocalHugeLongArray(nodeCount, tracker) : null;
    }

    private MultiSourceBFS(
        IdMapping nodeIds,
        MultiPartiteRelationshipIterator relationships,
        ExecutionStrategy strategy,
        long nodeCount,
        boolean allowStartNodeTraversal,
        CloseableThreadLocal<HugeLongArray> visits,
        CloseableThreadLocal<HugeLongArray> visitsNext,
        CloseableThreadLocal<HugeLongArray> seens,
        CloseableThreadLocal<HugeLongArray> seensNext,
        long... startNodes
    ) {
        assert startNodes != null && startNodes.length > 0;
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.strategy = strategy;
        this.startNodes = startNodes;
        this.nodeCount = nodeCount;
        this.allowStartNodeTraversal = allowStartNodeTraversal;
        this.visits = visits;
        this.visitsNext = visitsNext;
        this.seens = seens;
        this.seensNext = seensNext;
    }

    private MultiSourceBFS(
        IdMapping nodeIds,
        MultiPartiteRelationshipIterator relationships,
        ExecutionStrategy strategy,
        long nodeCount,
        long nodeOffset,
        int sourceNodeCount,
        boolean allowStartNodeTraversal,
        CloseableThreadLocal<HugeLongArray> visits,
        CloseableThreadLocal<HugeLongArray> visitsNext,
        CloseableThreadLocal<HugeLongArray> seens,
        CloseableThreadLocal<HugeLongArray> seensNext
    ) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.strategy = strategy;
        this.startNodes = null;
        this.nodeCount = nodeCount;
        this.nodeOffset = nodeOffset;
        this.sourceNodeCount = sourceNodeCount;
        this.allowStartNodeTraversal = allowStartNodeTraversal;
        this.visits = visits;
        this.visitsNext = visitsNext;
        this.seens = seens;
        this.seensNext = seensNext;
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
     * 64 startNodes. If there are more, {@link #run(int, ExecutorService)} must be used.
     */
    @Override
    public void run() {
        assert sourceLength() <= OMEGA : "more than " + OMEGA + " sources not supported";

        HugeLongArray visitSet = visits.get();
        HugeLongArray visitNextSet = visitsNext.get();
        HugeLongArray seenSet = seens.get();
        HugeLongArray seenNextSet = seensNext != null ? seensNext.get() : null;

        final SourceNodes sourceNodes;
        if (startNodes == null) {
            sourceNodes = prepareOffsetSources(visitSet, seenSet);
        } else {
            sourceNodes = prepareSpecifiedSources(visitSet, seenSet);
        }

        strategy.run(relationships, nodeCount, sourceNodes, visitSet, visitNextSet, seenSet, seenNextSet);
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
            if (!allowStartNodeTraversal) {
                seenSet.set(nodeId, 1L << i);
            }
            visitSet.or(nodeId, 1L << i);
        }

        return sourceNodes;
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
                        strategy,
                        sourceLength,
                        from,
                        length,
                        allowStartNodeTraversal,
                        visits,
                        visitsNext,
                        seens,
                        seensNext
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
                    strategy,
                    nodeCount,
                    allowStartNodeTraversal,
                    visits,
                    visitsNext,
                    seens,
                    seensNext,
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

    static final class SourceNodes implements BfsSources {
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
            assert sourceMask != 0;
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
            // reset() _always_ calls into fetchNext() which
            // finds the right-most set bit, updates pos to
            // its position and flips it. The correct size()
            // is therefore the number of set bits + 1.
            // Note, that this is under the assumption that
            // the source mask is never 0 on reset.
            return Long.bitCount(sourceMask) + 1;
        }

        private void fetchNext() {
            pos = Long.numberOfTrailingZeros(sourceMask);
            sourceMask ^= Long.lowestOneBit(sourceMask);
        }
    }

    private abstract static class ParallelMultiSources extends AbstractCollection<MultiSourceBFS> implements Iterator<MultiSourceBFS> {
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

    private static final class LocalHugeLongArray extends CloseableThreadLocal<HugeLongArray> {
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
