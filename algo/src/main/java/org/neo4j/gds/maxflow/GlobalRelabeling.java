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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

enum Phase {
    TRAVERSE,
    SYNC
}

public class GlobalRelabeling {
    public static void globalRelabeling(
        FlowGraph flowGraph,
        HugeLongArray label,
        long source,
        long target,
        Concurrency concurrency
    ) {
        label.setAll((i) -> flowGraph.nodeCount());
        label.set(target, 0L);
        var vertexIsDiscovered = HugeAtomicBitSet.create(flowGraph.nodeCount());

        var frontier = new AtomicWorkingSet(flowGraph.nodeCount());
        frontier.push(target);
        vertexIsDiscovered.set(target);
        vertexIsDiscovered.set(source);


        var tasks = ParallelUtil.tasks(
            concurrency,
            () -> new GlobalRelabellingBFSTask(flowGraph.concurrentCopy(), frontier, vertexIsDiscovered, label)
        );

        while (!frontier.isEmpty()) {
            RunWithConcurrency.builder().concurrency(concurrency).tasks(tasks).build().run();
            frontier.reset();
            RunWithConcurrency.builder().concurrency(concurrency).tasks(tasks).build().run();
        }
        label.set(source, flowGraph.nodeCount());
    }
}

class GlobalRelabellingBFSTask implements Runnable {
    private final FlowGraph flowGraph;
    private final AtomicWorkingSet frontier;
    private final HugeLongArrayQueue localDiscoveredVertices;
    private final HugeAtomicBitSet verticesIsDiscovered;
    private final HugeLongArray label;
    private final long batchSize;
    private final long LOCAL_QUEUE_BOUND = 128L;
    private Phase phase;

    GlobalRelabellingBFSTask(
        FlowGraph flowGraph,
        AtomicWorkingSet frontier,
        HugeAtomicBitSet vertexIsDiscovered,
        HugeLongArray label
    ) {
        this.flowGraph = flowGraph;
        this.frontier = frontier;
        this.localDiscoveredVertices = HugeLongArrayQueue.newQueue(flowGraph.nodeCount()); //think
        this.verticesIsDiscovered = vertexIsDiscovered;
        this.label = label;
        this.phase = Phase.TRAVERSE;
        this.batchSize = 1024L;
    }

    @Override
    public void run() {
        if (phase == Phase.TRAVERSE) {
            traverse();
        } else {
            addToFrontier();
        }
    }

    private void singleTraverse(long v) {
        var newLabel = label.get(v) + 1;
        flowGraph.forEachRelationship(
            v, (s, t, relIdx, residualCapacity, isReverse) -> {
                //(s)-->(t) //want t-->s to have free capacity. (can push from t to s)
                if (flowGraph.residualCapacity(relIdx, isReverse) <= 0.0) {
                    return true;
                }
                if (!verticesIsDiscovered.getAndSet(t)) {
                    localDiscoveredVertices.add(t);
                    label.set(t, newLabel);
                }
                return true;
            }
        );
    }

    public void traverse() {
        long oldIdx;
        while ((oldIdx = frontier.getAndAdd(batchSize)) < frontier.size()) {
            long toIdx = Math.min(oldIdx + batchSize, frontier.size());
            frontier.consumeBatch(oldIdx, toIdx, this::singleTraverse);
        }

        //do some local processing if the localQueue is small enough
        while (!localDiscoveredVertices.isEmpty() && localDiscoveredVertices.size() < LOCAL_QUEUE_BOUND) {
            long nodeId = localDiscoveredVertices.remove();
            singleTraverse(nodeId);
        }
        phase = Phase.SYNC;
    }

    public void addToFrontier() {
        frontier.batchPush(localDiscoveredVertices);
        phase = Phase.TRAVERSE;
    }
}
