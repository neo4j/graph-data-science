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
package org.neo4j.gds.impl.traverse;


import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.neo4j.gds.impl.Converters.longToIntConsumer;

public final class BFS extends Algorithm<long[]> {

    public static final Aggregator DEFAULT_AGGREGATOR = (s, t, w) -> .0;

    private final long startNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;

    private final Graph graph;

    private HugeLongArray nodes;
    private final HugeLongArray sources;
    private HugeDoubleArray weights;

    private HugeAtomicBitSet visited;
    private final int concurrency;

    public BFS(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.startNodeId = startNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.concurrency = concurrency;

        var nodeCount = Math.toIntExact(graph.nodeCount());

        this.nodes = HugeLongArray.newArray(nodeCount);
        this.sources = HugeLongArray.newArray(nodeCount);
        this.weights = HugeDoubleArray.newArray(nodeCount);
        this.visited = HugeAtomicBitSet.create(nodeCount);
    }

    @Override
    public long[] compute() {
        progressTracker.beginSubTask();
        AtomicInteger nodesIndex = new AtomicInteger(0);
        visited.set(startNodeId);
        nodes.set(0, startNodeId);
        AtomicInteger nodesLength = new AtomicInteger(1);
        sources.set(0, startNodeId);
        weights.set(0, 0);

        var bfsTaskList = new ArrayList<BFSTask>(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            bfsTaskList.add(new BFSTask(graph, nodes, nodesIndex, nodesLength, visited, sources, weights));
        }

        while (running()) {
            ParallelUtil.run(bfsTaskList, Pools.DEFAULT);
            var shouldBreak = bfsTaskList.stream().anyMatch(BFSTask::shouldBreak);
            if (shouldBreak) {
                break;
            }

            int oldNodesLength = nodesLength.get();

            bfsTaskList.forEach(bfsTask -> bfsTask.setPhase(Phase.SYNC));
            ParallelUtil.run(bfsTaskList, Pools.DEFAULT);
            bfsTaskList.forEach(bfsTask -> bfsTask.setPhase(Phase.COMPUTE));

            nodesIndex.set(oldNodesLength);

            if (nodesLength.get() == oldNodesLength) {
                break;
            }
        }
        long[] resultNodes = nodes.copyOf(nodesLength.get()).toArray();
        resultNodes = Arrays.stream(resultNodes).filter(node -> node >= 0).toArray();
        progressTracker.endSubTask();
        return resultNodes;
    }


    @Override
    public void release() {
        nodes = null;
        weights = null;
        visited = null;
    }

    enum Phase {
        COMPUTE, SYNC
    }

    class BFSTask implements Runnable {
        private final Graph graph;
        private final AtomicInteger nodesIndex;
        private final HugeAtomicBitSet visited;
        private final HugeLongArray nodes;
        private final HugeLongArray sources;
        private final HugeDoubleArray weights;
        private final LongArrayList localNodes;
        private final LongArrayList localSources;
        private final DoubleArrayList localWeights;

        private final AtomicInteger nodesLength;

        private Phase phase;

        private boolean shouldBreak = false;

        BFSTask(
            Graph graph,
            HugeLongArray nodes,
            AtomicInteger nodesIndex,
            AtomicInteger nodesLength,
            HugeAtomicBitSet visited,
            HugeLongArray sources,
            HugeDoubleArray weights
        ) {
            this.graph = graph.concurrentCopy();
            this.nodesIndex = nodesIndex;
            this.nodesLength = nodesLength;
            this.visited = visited;
            this.nodes = nodes;
            this.sources = sources;
            this.weights = weights;
            this.localNodes = new LongArrayList();
            this.localSources = new LongArrayList();
            this.localWeights = new DoubleArrayList();
            this.phase = Phase.COMPUTE;
        }

        public void run() {
            if (phase == Phase.COMPUTE)
                compute();
            else
                syncFromLocalToGlobal();
        }

        void setPhase(Phase phase) {
            this.phase = phase;
        }

        private void compute() {
            int offset;
            while ((offset = nodesIndex.getAndAdd(64)) < nodesLength.get()) {
                int limit = Math.min(offset + 64, nodesLength.get());
                for (int idx = offset; idx < limit; idx++) {
                    var nodeId = nodes.get(idx);
                    var sourceId = sources.get(idx);
                    var weight = weights.get(idx);
                    var keepNode = relaxNode(nodeId, sourceId, weight);
                    if (!keepNode) {
                        nodes.set(idx, -1);
                    }
                }
            }
        }

        private void syncFromLocalToGlobal() {
            if (!localNodes.isEmpty()) {
                var size = localNodes.size();
                var offset = nodesLength.getAndAdd(size);
                for (LongCursor longCursor : localNodes) {
                    int index = offset + longCursor.index;
                    nodes.set(index, longCursor.value);
                    sources.set(index, localSources.get(longCursor.index));
                    weights.set(index, localWeights.get(longCursor.index));
                }
                localNodes.elementsCount = 0;
                localSources.elementsCount = 0;
                localWeights.elementsCount = 0;
            }
        }

        boolean shouldBreak() {
            return shouldBreak;
        }

        boolean relaxNode(long node, long source, double weight) {
            var exitPredicateResult = exitPredicate.test(source, node, weight);
            if (exitPredicateResult == ExitPredicate.Result.CONTINUE) {
                return false;
            } else {
                if (exitPredicateResult == ExitPredicate.Result.BREAK) {
                    shouldBreak = true;
                    return true;
                }
            }
            this.graph.forEachRelationship(
                node,
                longToIntConsumer((s, t) -> {
                    if (!visited.getAndSet(t)) {
                        localSources.add(s);
                        localNodes.add(t);
                        localWeights.add(aggregatorFunction.apply(s, t, weight));
                    }
                    return running();
                })
            );
            return true;
        }

    }
}
