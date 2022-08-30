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
package org.neo4j.gds.betweenness;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class BetweennessCentrality extends Algorithm<HugeAtomicDoubleArray> {

    private final Graph graph;
    private final AtomicLong nodeQueue = new AtomicLong();
    private final long nodeCount;
    private final double divisor;

    private HugeAtomicDoubleArray centrality;
    private SelectionStrategy selectionStrategy;

    private final ExecutorService executorService;
    private final int concurrency;

    public BetweennessCentrality(
        Graph graph,
        SelectionStrategy selectionStrategy,
        ExecutorService executorService,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.nodeCount = graph.nodeCount();
        this.centrality = HugeAtomicDoubleArray.newArray(nodeCount);
        this.selectionStrategy = selectionStrategy;
        this.selectionStrategy.init(graph, executorService, concurrency);
        this.divisor = graph.schema().isUndirected() ? 2.0 : 1.0;
    }

    @Override
    public HugeAtomicDoubleArray compute() {
        progressTracker.beginSubTask();
        nodeQueue.set(0);
        ParallelUtil.run(ParallelUtil.tasks(concurrency, () -> new BCTask()), executorService);
        progressTracker.endSubTask();
        return centrality;
    }

    @Override
    public void release() {
        centrality = null;
        selectionStrategy = null;
    }

    final class BCTask implements Runnable {

        private final RelationshipIterator localRelationshipIterator;

        private final HugeObjectArray<LongArrayList> predecessors;
        private final HugeCursor<LongArrayList[]> predecessorsCursor;

        private final HugeLongArrayQueue forwardNodes;
        private final HugeLongArrayStack backwardNodes;

        private final HugeDoubleArray delta;
        private final HugeLongArray sigma;
        private final HugeIntArray distance;

        private BCTask() {
            this.localRelationshipIterator = graph.concurrentCopy();

            this.predecessors = HugeObjectArray.newArray(LongArrayList.class, nodeCount);
            this.predecessorsCursor = predecessors.newCursor();
            this.backwardNodes = HugeLongArrayStack.newStack(nodeCount);
            // TODO: make queue growable
            this.forwardNodes = HugeLongArrayQueue.newQueue(nodeCount);

            this.sigma = HugeLongArray.newArray(nodeCount);
            this.delta = HugeDoubleArray.newArray(nodeCount);
            this.distance = HugeIntArray.newArray(nodeCount);
        }

        @Override
        public void run() {
            for (;;) {
                // take start node from the queue
                long startNodeId = nodeQueue.getAndIncrement();
                if (startNodeId >= nodeCount || !terminationFlag.running()) {
                    return;
                }
                // check whether the node is part of the subset
                if (!selectionStrategy.select(startNodeId)) {
                    continue;
                }
                // reset
                getProgressTracker().logProgress();

                clear();

                sigma.addTo(startNodeId, 1);
                distance.set(startNodeId, 0);

                forwardNodes.add(startNodeId);

                // BC forward traversal
                while (!forwardNodes.isEmpty()) {
                    long node = forwardNodes.remove();
                    backwardNodes.push(node);
                    int distanceNode = distance.get(node);

                    localRelationshipIterator.forEachRelationship(node, (source, target) -> {
                        if (distance.get(target) < 0) {
                            forwardNodes.add(target);
                            distance.set(target, distanceNode + 1);
                        }

                        if (distance.get(target) == distanceNode + 1) {
                            sigma.addTo(target, sigma.get(source));
                            append(target, source);
                        }
                        return true;
                    });
                }

                while (!backwardNodes.isEmpty()) {
                    long node = backwardNodes.pop();
                    LongArrayList predecessors = this.predecessors.get(node);

                    double dependencyNode = delta.get(node);
                    double sigmaNode = sigma.get(node);

                    if (null != predecessors) {
                        predecessors.forEach((Consumer<? super LongCursor>) predecessor -> {
                            double sigmaPredecessor = sigma.get(predecessor.value);
                            double dependency = sigmaPredecessor / sigmaNode * (dependencyNode + 1.0);
                            delta.addTo(predecessor.value, dependency);
                        });
                    }
                    if (node != startNodeId) {
                        double current;
                        do {
                            current = centrality.get(node);
                        } while (!centrality.compareAndSet(node, current, current + dependencyNode / divisor));
                    }
                }
            }
        }

        // append node to the path at target
        private void append(long target, long node) {
            LongArrayList targetPredecessors = predecessors.get(target);
            if (null == targetPredecessors) {
                targetPredecessors = new LongArrayList();
                predecessors.set(target, targetPredecessors);
            }
            targetPredecessors.add(node);
        }

        private void clear() {
            distance.fill(-1);
            sigma.fill(0);
            delta.fill(0);

            predecessors.initCursor(predecessorsCursor);

            while (predecessorsCursor.next()) {
                for (int i = predecessorsCursor.offset; i < predecessorsCursor.limit; i++) {
                    if (predecessorsCursor.array[i] != null) {
                        // We avoid using LongArrayList#clear since it would
                        // fill the inner array with zeros. We don't need that
                        // so we just reset the index which is cheaper
                        predecessorsCursor.array[i].elementsCount = 0;
                    }
                }
            }
        }
    }
}
