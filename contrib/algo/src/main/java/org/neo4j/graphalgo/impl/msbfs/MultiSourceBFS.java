/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
public final class MultiSourceBFS implements Runnable, MsBFSAlgo {

    // how many sources can be traversed simultaneously
    static final int OMEGA = 64;

    private final ThreadLocal<long[]> visits;
    private final ThreadLocal<long[]> nexts;
    private final ThreadLocal<long[]> seens;

    private final IdMapping nodeIds;
    private final RelationshipIterator relationships;
    private final Direction direction;
    private final BfsConsumer perNodeAction;
    private final int[] startNodes;
    private int nodeOffset, sourceNodeCount;
    private int nodeCount;

    public MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            int... startNodes) {
        this.nodeIds = nodeIds;
        this.relationships = relationships;
        this.direction = direction;
        this.perNodeAction = perNodeAction;
        this.startNodes = (startNodes != null && startNodes.length > 0) ? startNodes : null;
        if (this.startNodes != null) {
            Arrays.sort(this.startNodes);
        }
        nodeCount = Math.toIntExact(nodeIds.nodeCount());
        this.visits = new LocalLongArray(nodeCount);
        this.nexts = new LocalLongArray(nodeCount);
        this.seens = new LocalLongArray(nodeCount);
    }

    private MultiSourceBFS(
            IdMapping nodeIds,
            RelationshipIterator relationships,
            Direction direction,
            BfsConsumer perNodeAction,
            int nodeCount,
            ThreadLocal<long[]> visits,
            ThreadLocal<long[]> nexts,
            ThreadLocal<long[]> seens,
            int... startNodes) {
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
            int nodeCount,
            int nodeOffset,
            int sourceNodeCount,
            ThreadLocal<long[]> visits,
            ThreadLocal<long[]> nexts,
            ThreadLocal<long[]> seens) {
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
    @Override
    public void run(int concurrency, ExecutorService executor) {
        int sourceLength = sourceLength();
        int threads = ParallelUtil.threadSize(OMEGA, sourceLength);
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

    private int sourceLength() {
        if (startNodes != null) {
            return startNodes.length;
        }
        if (sourceNodeCount == 0) {
            return nodeCount;
        }
        return sourceNodeCount;
    }

    /**
     * Runs MS-BFS, always single-threaded. Requires that there are at most
     * 64 startNodes. If there are more, {@link #run(int, ExecutorService)} must be used.
     */
    @Override
    public void run() {
        assert sourceLength() <= OMEGA : "more than " + OMEGA + " sources not supported";

        int totalNodeCount = this.nodeCount;

        long[] visitSet = visits.get();
        long[] nextSet = nexts.get();
        long[] seenSet = seens.get();

        final SourceNodes sourceNodes;
        if (startNodes == null) {
            sourceNodes = prepareOffsetSources(visitSet, seenSet);
        } else {
            sourceNodes = prepareSpecifiedSources(visitSet, seenSet);
        }

        runLocalMsbfs(totalNodeCount, sourceNodes, visitSet, nextSet, seenSet);
    }

    private SourceNodes prepareOffsetSources(final long[] visitSet, final long[] seenSet) {
        int localNodeCount = this.sourceNodeCount;
        int nodeOffset = this.nodeOffset;
        SourceNodes sourceNodes = new SourceNodes(nodeOffset, localNodeCount);

        for (int i = 0; i < localNodeCount; ++i) {
            seenSet[nodeOffset + i] = (1L << i);
        }
        for (int i = 0; i < localNodeCount; ++i) {
            visitSet[nodeOffset + i] |= (1L << i);
        }

        return sourceNodes;
    }

    private SourceNodes prepareSpecifiedSources(final long[] visitSet, final long[] seenSet) {
        assert isSorted(startNodes);

        int[] startNodes = this.startNodes;
        int localNodeCount = startNodes.length;
        SourceNodes sourceNodes = new SourceNodes(startNodes);

        for (int i = 0; i < localNodeCount; ++i) {
            int nodeId = startNodes[i];
            seenSet[nodeId] = (1L << i);
            visitSet[nodeId] |= (1L << i);
        }

        return sourceNodes;
    }

    private void runLocalMsbfs(
            int totalNodeCount,
            SourceNodes sourceNodes,
            long[] visitSet,
            long[] nextSet,
            long[] seenSet) {

        int depth = 0;

        while (true) {
            for (int i = 0; i < totalNodeCount; ++i) {
                if (visitSet[i] != 0L) {
                    prepareNextVisit(visitSet[i], i, nextSet);
                }
            }

            ++depth;

            boolean hasNext = false;
            long next;
            for (int i = 0; i < totalNodeCount; ++i) {
                if (nextSet[i] != 0L) {
                    next = visitNext(i, seenSet, nextSet);
                    if (next != 0L) {
                        sourceNodes.reset(next);
                        perNodeAction.accept(i, depth, sourceNodes);
                        hasNext = true;
                    }
                }
            }

            if (!hasNext) {
                return;
            }

            System.arraycopy(nextSet, 0, visitSet, 0, totalNodeCount);
            Arrays.fill(nextSet, 0L);
        }
    }

    private void prepareNextVisit(long nodeVisit, int nodeId, long[] nextSet) {
        relationships.forEachRelationship(
                nodeId,
                direction,
                (src, tgt, rel) -> {
                    nextSet[tgt] |= nodeVisit;
                    return true;
                });
    }

    private void prepareNextVisit(long nodeVisit, int nodeId, long[] nextSet, long[] seenSet) {
        relationships.forEachRelationship(
                nodeId,
                direction,
                (src, tgt, rel) -> {
                    long toVisit = nodeVisit & ~seenSet[tgt];
                    if (toVisit != 0L) {
                        nextSet[tgt] |= nodeVisit;
                    }
                    return true;
                });
    }

    private long visitNext(int nodeId, long[] seenSet, long[] nextSet) {
        long seen = seenSet[nodeId];
        long next = nextSet[nodeId] &= ~seen;
        seenSet[nodeId] |= next;
        return next;
    }

    /* assert-only */ private boolean isSorted(int[] nodes) {
        int[] copy = Arrays.copyOf(nodes, nodes.length);
        Arrays.sort(copy);
        return Arrays.equals(copy, nodes);
    }

    // lazily creates MS-BFS instances for OMEGA sized source chunks
    private Collection<MultiSourceBFS> allSourceBfss(int threads) {
        if (startNodes == null) {
            int sourceLength = nodeCount;
            return new ParallelMultiSources(threads, sourceLength) {
                @Override
                MultiSourceBFS next(final int from, final int length) {
                    return new MultiSourceBFS(
                            nodeIds,
                            relationships,
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
        int[] startNodes = this.startNodes;
        int sourceLength = startNodes.length;
        return new ParallelMultiSources(threads, sourceLength) {
            @Override
            MultiSourceBFS next(final int from, final int length) {
                return new MultiSourceBFS(
                        nodeIds,
                        relationships,
                        direction,
                        perNodeAction,
                        nodeCount,
                        visits,
                        nexts,
                        seens,
                        Arrays.copyOfRange(startNodes, from, from + length)
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
        private final int[] sourceNodes;
        private final int maxPos;
        private final int startPos;
        private final int offset;
        private long sourceMask;
        private int pos;

        private SourceNodes(int[] sourceNodes) {
            assert sourceNodes.length <= OMEGA;
            this.sourceNodes = sourceNodes;
            this.maxPos = sourceNodes.length;
            this.offset = 0;
            this.startPos = -1;
        }

        private SourceNodes(int offset, int length) {
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
        public int next() {
            int current = this.pos;
            fetchNext();
            return sourceNodes != null ? sourceNodes[current] : current + offset;
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
        private final int sourceLength;
        private int start = 0;
        private int i = 0;

        private ParallelMultiSources(int threads, int sourceLength) {
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
            start = 0;
            i = 0;
            return this;
        }

        @Override
        public MultiSourceBFS next() {
            int len = Math.min(OMEGA, sourceLength - start);
            MultiSourceBFS bfs = next(start, len);
            start += len;
            i++;
            return bfs;
        }

        abstract MultiSourceBFS next(int from, int length);
    }

    private static final class LocalLongArray extends ThreadLocal<long[]> {
        private final int size;

        private LocalLongArray(final int size) {
            this.size = size;
        }

        @Override
        protected long[] initialValue() {
            return new long[size];
        }

        @Override
        public long[] get() {
            long[] values = super.get();
            Arrays.fill(values, 0L);
            return values;
        }
    }
}
