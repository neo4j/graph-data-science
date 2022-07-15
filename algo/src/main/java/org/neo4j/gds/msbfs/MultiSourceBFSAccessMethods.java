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
package org.neo4j.gds.msbfs;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.Arrays;
import java.util.Collection;
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
public final class MultiSourceBFSAccessMethods {

    private final CloseableThreadLocal<HugeLongArray> visits;
    private final CloseableThreadLocal<HugeLongArray> visitsNext;
    private final CloseableThreadLocal<HugeLongArray> seens;
    private final @Nullable CloseableThreadLocal<HugeLongArray> seensNext;

    private final long nodeCount;
    private final RelationshipIterator relationships;
    private final ExecutionStrategy strategy;
    private final boolean allowStartNodeTraversal;
    private final long @Nullable [] sourceNodes;

    // hypothesis: you supply actual source nodes, or you provide a count - if so that should be rationalised
    private final int sourceNodeCount;
    private final long nodeOffset;

    public static MultiSourceBFSAccessMethods aggregatedNeighborProcessingWithoutSourceNodes(
        long nodeCount,
        RelationshipIterator relationships,
        BfsConsumer perNodeAction
    ) {
        return createWithoutSeensNextOrSourceNodesOrStartNodeTraversal(
            nodeCount,
            relationships,
            new ANPStrategy(perNodeAction)
        );
    }

    public static MultiSourceBFSAccessMethods aggregatedNeighborProcessing(
        long nodeCount,
        RelationshipIterator relationships,
        BfsConsumer perNodeAction,
        long[] sourceNodes
    ) {
        return createWithoutSeensNextOrStartNodeTraversal(
            nodeCount,
            relationships,
            new ANPStrategy(perNodeAction),
            sourceNodes
        );
    }

    // only used from tests
    public static MultiSourceBFSAccessMethods predecessorProcessingWithoutSourceNodes(
        Graph graph,
        BfsConsumer perNodeAction,
        BfsWithPredecessorConsumer perNeighborAction
    ) {
        return createWithoutSourceNodesOrStartNodeTraversal(
            graph.nodeCount(),
            graph,
            new PredecessorStrategy(perNodeAction, perNeighborAction)
        );
    }

    public static MultiSourceBFSAccessMethods predecessorProcessing(
        Graph graph,
        BfsConsumer perNodeAction,
        BfsWithPredecessorConsumer perNeighborAction,
        long[] sourceNodes
    ) {
        return createWithoutStartNodeTraversal(
            graph.nodeCount(),
            graph,
            new PredecessorStrategy(perNodeAction, perNeighborAction),
            sourceNodes
        );
    }

    private static MultiSourceBFSAccessMethods createWithoutSeensNextOrSourceNodesOrStartNodeTraversal(
        long nodeCount,
        RelationshipIterator relationships,
        ExecutionStrategy strategy
    ) {
        var visits = new LocalHugeLongArray(nodeCount);
        var visitsNext = new LocalHugeLongArray(nodeCount);
        var seens = new LocalHugeLongArray(nodeCount);

        return new MultiSourceBFSAccessMethods(
            visits,
            visitsNext,
            seens,
            null,
            nodeCount,
            relationships,
            strategy,
            false,
            null,
            0,
            0
            );
    }

    private static MultiSourceBFSAccessMethods createWithoutSeensNextOrStartNodeTraversal(
        long nodeCount,
        RelationshipIterator relationships,
        ExecutionStrategy strategy,
        long[] sourceNodes
    ) {
        var visits = new LocalHugeLongArray(nodeCount);
        var visitsNext = new LocalHugeLongArray(nodeCount);
        var seens = new LocalHugeLongArray(nodeCount);

        return new MultiSourceBFSAccessMethods(
            visits,
            visitsNext,
            seens,
            null,
            nodeCount,
            relationships,
            strategy,
            false,
            sourceNodes,
            0,
            0
            );
    }

    private static MultiSourceBFSAccessMethods createWithoutSourceNodesOrStartNodeTraversal(
        long nodeCount,
        RelationshipIterator relationships,
        ExecutionStrategy strategy
    ) {
        var visits = new LocalHugeLongArray(nodeCount);
        var visitsNext = new LocalHugeLongArray(nodeCount);
        var seens = new LocalHugeLongArray(nodeCount);
        var seensNext = new LocalHugeLongArray(nodeCount);

        return new MultiSourceBFSAccessMethods(
            visits,
            visitsNext,
            seens,
            seensNext,
            nodeCount,
            relationships,
            strategy,
            false,
            null,
            0,
            0
            );
    }

    /**
     * @param sourceNodes at least one source node must be provided
     */
    private static MultiSourceBFSAccessMethods createWithoutStartNodeTraversal(
        long nodeCount,
        RelationshipIterator relationships,
        ExecutionStrategy strategy,
        long[] sourceNodes
    ) {
        if (sourceNodes.length == 0) {
            throw new IllegalArgumentException("You must provide source nodes");
        }

        Arrays.sort(sourceNodes);

        var visits = new LocalHugeLongArray(nodeCount);
        var visitsNext = new LocalHugeLongArray(nodeCount);
        var seens = new LocalHugeLongArray(nodeCount);
        var seensNext = new LocalHugeLongArray(nodeCount);

        return new MultiSourceBFSAccessMethods(
            visits,
            visitsNext,
            seens,
            seensNext,
            nodeCount,
            relationships,
            strategy,
            false,
            sourceNodes,
            0,
            0
            );
    }

    /**
     * There is just one constructor, and it only does assignments.
     */
    private MultiSourceBFSAccessMethods(
        CloseableThreadLocal<HugeLongArray> visits,
        CloseableThreadLocal<HugeLongArray> visitsNext,
        CloseableThreadLocal<HugeLongArray> seens,
        @Nullable CloseableThreadLocal<HugeLongArray> seensNext,
        long nodeCount,
        RelationshipIterator relationships,
        ExecutionStrategy strategy,
        boolean allowStartNodeTraversal,
        long @Nullable [] sourceNodes,
        int sourceNodeCount,
        long nodeOffset
    ) {
        this.visits = visits;
        this.visitsNext = visitsNext;
        this.seens = seens;
        this.seensNext = seensNext;
        this.nodeCount = nodeCount;
        this.relationships = relationships;
        this.strategy = strategy;
        this.allowStartNodeTraversal = allowStartNodeTraversal;
        this.sourceNodes = sourceNodes;
        this.sourceNodeCount = sourceNodeCount;
        this.nodeOffset = nodeOffset;
    }

    /**
     * Runs MS-BFS, possibly in parallel.
     */
    public void run(int concurrency, ExecutorService executor) {
        final int threads = numberOfThreads();
        var bfss = allSourceBfss(threads);

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(bfss)
            .maxWaitRetries((long) threads << 2)
            .waitTime(100L, TimeUnit.MICROSECONDS)
            .executor(executor)
            .run();
    }

    private long sourceLength() {
        if (sourceNodes != null) {
            return sourceNodes.length;
        }
        if (sourceNodeCount == 0) {
            return nodeCount;
        }
        return sourceNodeCount;
    }

    private int numberOfThreads() {
        long sourceLength = sourceLength();
        long threads = ParallelUtil.threadCount(MSBFSConstants.OMEGA, sourceLength);
        if ((int) threads != threads) {
            throw new IllegalArgumentException("Unable run MS-BFS on " + sourceLength + " sources.");
        }
        return (int) threads;
    }

    // lazily creates MS-BFS instances for OMEGA sized source chunks
    private Collection<MultiSourceBFSRunnable> allSourceBfss(int threads) {
        if (sourceNodes == null) {
            long sourceLength = nodeCount;
            return new ParallelMultiSources(threads, sourceLength) {
                @Override
                MultiSourceBFSRunnable next(final long from, final int length) {
                    return new MultiSourceBFSRunnable(
                        visits,
                        visitsNext,
                        seens,
                        seensNext,
                        sourceLength,
                        relationships.concurrentCopy(),
                        strategy,
                        allowStartNodeTraversal,
                        null,
                        length,
                        from
                    );
                }
            };
        }
        long[] sourceNodes = this.sourceNodes;
        int sourceLength = sourceNodes.length;
        return new ParallelMultiSources(threads, sourceLength) {
            @Override
            MultiSourceBFSRunnable next(final long from, final int length) {
                return new MultiSourceBFSRunnable(
                    visits,
                    visitsNext,
                    seens,
                    seensNext,
                    nodeCount,
                    relationships.concurrentCopy(),
                    strategy,
                    allowStartNodeTraversal,
                    Arrays.copyOfRange(sourceNodes, (int) from, (int) (from + length)),
                    0,
                    0
                );
            }
        };
    }

    @Override
    public String toString() {
        if (sourceNodes != null && sourceNodes.length > 0) {
            return "MSBFS{" + sourceNodes[0] +
                   " .. " + (sourceNodes[sourceNodes.length - 1] + 1) +
                   " (" + sourceNodes.length +
                   ")}";
        }
        return "MSBFS{" + nodeOffset +
                " .. " + (nodeOffset + sourceNodeCount) +
                " (" + sourceNodeCount +
                ")}";
    }
}
