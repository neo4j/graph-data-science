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
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.utils.CloseableThreadLocal;

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
public final class MultiSourceBFSRunnable implements Runnable {

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

    public static MultiSourceBFSRunnable createWithoutSeensNext(
        long nodeCount,
        RelationshipIterator relationships,
        ExecutionStrategy strategy,
        boolean allowStartNodeTraversal,
        long[] sourceNodes
    ) {
        var visits = new LocalHugeLongArray(nodeCount);
        var visitsNext = new LocalHugeLongArray(nodeCount);
        var seens = new LocalHugeLongArray(nodeCount);

        return new MultiSourceBFSRunnable(
            visits,
            visitsNext,
            seens,
            null,
            nodeCount,
            relationships,
            strategy,
            allowStartNodeTraversal,
            sourceNodes,
            0,
            0
        );
    }

    /**
     * There is just one constructor, and it only does assignments.
     */
    MultiSourceBFSRunnable(
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
     * Runs MS-BFS, always single-threaded. Requires that there are at most 64 source nodes.
     */
    @Override
    public void run() {
        var visitSet = visits.get();
        var visitNextSet = visitsNext.get();
        var seenSet = seens.get();
        var seenNextSet = seensNext != null ? seensNext.get() : null;

        SourceNodes sourceNodes = this.sourceNodes == null
            ? prepareOffsetSources(visitSet, seenSet)
            : prepareSpecifiedSources(visitSet, seenSet, this.sourceNodes, allowStartNodeTraversal);

        strategy.run(relationships, nodeCount, sourceNodes, visitSet, visitNextSet, seenSet, seenNextSet);
    }

    private SourceNodes prepareOffsetSources(HugeLongArray visitSet, HugeLongArray seenSet) {
        var localNodeCount = this.sourceNodeCount;
        var nodeOffset = this.nodeOffset;

        for (int i = 0; i < localNodeCount; ++i) {
            seenSet.set(nodeOffset + i, 1L << i);
            visitSet.or(nodeOffset + i, 1L << i);
        }

        return new SourceNodes(nodeOffset, localNodeCount);
    }

    private static SourceNodes prepareSpecifiedSources(
        HugeLongArray visitSet,
        HugeLongArray seenSet,
        long[] sourceNodes,
        boolean allowStartNodeTraversal
    ) {
        for (int i = 0; i < sourceNodes.length; ++i) {
            long nodeId = sourceNodes[i];
            if (!allowStartNodeTraversal) {
                seenSet.set(nodeId, 1L << i);
            }
            visitSet.or(nodeId, 1L << i);
        }

        return new SourceNodes(sourceNodes);
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
