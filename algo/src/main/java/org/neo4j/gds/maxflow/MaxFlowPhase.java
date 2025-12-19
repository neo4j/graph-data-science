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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mcmf.MinCostFunctions;
import org.neo4j.gds.termination.TerminationFlag;

public class MaxFlowPhase {

    private final FlowGraph flowGraph;
    private final HugeDoubleArray excess;
    private final HugeLongArray label;
    private final MaxFlowParameters parameters;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public MaxFlowPhase(FlowGraph flowGraph, HugeDoubleArray excess,
        MaxFlowParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.flowGraph = flowGraph;
        this.excess = excess;
        this.label = HugeLongArray.newArray(flowGraph.nodeCount());
        this.parameters = parameters;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    private void initPreflow() {
        flowGraph.forEachRelationship(
            flowGraph.superSource(), (s, t, relIdx, residualCapacity, isReverse) -> {
                if (MinCostFunctions.isResidualEdge(residualCapacity)) {
                    flowGraph.push(relIdx, residualCapacity, isReverse);
                    excess.set(t, residualCapacity);
                }
                return true;
            }
        );

    }

    public void computeMaxFlow() {
        initPreflow();
        maximizeFlowSequential(flowGraph.superSource(), flowGraph.superTarget());
        maximizeFlowSequential(flowGraph.superTarget(), flowGraph.superSource());
    }

    private void maximizeFlowSequential(long sourceNode, long targetNode) {
        var nodeCount = flowGraph.nodeCount();
        label.set(sourceNode, nodeCount);

        var workingQueue = HugeLongArrayQueue.newQueue(nodeCount);
        var inWorkingQueue = new BitSet(nodeCount); //need not be atomic atm
        var totalExcess = 0D;
        for (var nodeId = 0; nodeId < flowGraph.nodeCount(); nodeId++) {
            if (nodeId == flowGraph.superSource() || nodeId == flowGraph.superTarget()) continue;
            if (MaxFlowFunctions.treatAsPositive(excess.get(nodeId))) {
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
            terminationFlag
        );

        var gapDetector = GapFactory.create(parameters.useGapRelabelling(), nodeCount, label, parameters.concurrency());

        var discharging = new SequentialDischarging(
            flowGraph,
            excess,
            label,
            workingQueue,
            inWorkingQueue,
            globalRelabeling,
            gapDetector,
            parameters.freq(),
            excessAtDestinations,
            totalExcess,
            progressTracker,
            terminationFlag
        );
        discharging.dischargeUntilDone();
    }

}
