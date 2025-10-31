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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

public final class MaxFlow extends Algorithm<FlowResult> {
    private final Graph graph;
    private final MaxFlowParameters parameters;

    private FlowGraph flowGraph;
    private HugeDoubleArray excess;
    private HugeLongArray label;

    public MaxFlow(
        Graph graph,
        MaxFlowParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.graph = graph;
        this.parameters = parameters;
        this.terminationFlag = terminationFlag;
    }

    public FlowResult compute() {
        progressTracker.beginSubTask();
        initPreflow();
        maximizeFlowSequential(flowGraph.superSource(), flowGraph.superTarget());
        maximizeFlowSequential(flowGraph.superTarget(), flowGraph.superSource());
        progressTracker.endSubTask();
        return flowGraph.createFlowResult();
    }

    private void initPreflow() {
        var supplyAndDemand = SupplyAndDemandFactory.create(graph, parameters.sourceNodes(), parameters.targetNodes());
        flowGraph = FlowGraph.create(graph, supplyAndDemand.getLeft(), supplyAndDemand.getRight(), terminationFlag);
        excess = HugeDoubleArray.newArray(flowGraph.nodeCount());
        excess.setAll(x -> 0D);
        flowGraph.forEachRelationship(
            flowGraph.superSource(), (s, t, relIdx, residualCapacity, isReverse) -> {
                flowGraph.push(relIdx, residualCapacity, isReverse);
                excess.set(t, residualCapacity);
                return true;
            }
        );
        label = HugeLongArray.newArray(flowGraph.nodeCount());
    }

    private void maximizeFlowSequential(long sourceNode, long targetNode) {
        var nodeCount = flowGraph.nodeCount();
        label.set(sourceNode, nodeCount);

        var workingQueue = HugeLongArrayQueue.newQueue(nodeCount);
        var inWorkingQueue = HugeAtomicBitSet.create(nodeCount); //need not be atomic atm
        var totalExcess = 0D;
        for (var nodeId = 0; nodeId < flowGraph.originalNodeCount(); nodeId++) {
            if (excess.get(nodeId) > 0.0) {
                workingQueue.add(nodeId);
                inWorkingQueue.set(nodeId);
                totalExcess += excess.get(nodeId);
            }
        }
        var excessAtDestinations = excess.get(sourceNode) + excess.get(targetNode);
        inWorkingQueue.set(targetNode); //it's not, but we don't want to add it

        HugeLongArrayQueue[] threadQueues = new HugeLongArrayQueue[parameters.concurrency().value()];
        for (int i = 0; i < threadQueues.length; i++) {
            threadQueues[i] = HugeLongArrayQueue.newQueue(flowGraph.nodeCount());
        }

        var globalRelabeling = GlobalRelabeling.createRelabeling(
            flowGraph,
            label,
            sourceNode,
            targetNode,
            parameters.concurrency(),
            threadQueues,
            TerminationFlag.RUNNING_TRUE
        );

        var discharging = new SequentialDischarging(
            flowGraph,
            excess,
            label,
            workingQueue,
            inWorkingQueue,
            globalRelabeling,
            parameters.freq(),
            true,
            excessAtDestinations,
            totalExcess,
            progressTracker
        );
        discharging.dischargeUntilDone();
    }
}
