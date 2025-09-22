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
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public final class MaxFlow extends Algorithm<FlowResult> {
    static final double FREQ = 0.5;
    static final int ALPHA = 6;
    static final int BETA = 12;
    private final Graph graph;
    private final long source;
    private final long target;
    private final MaxFlowParameters parameters;
    private final ExecutorService executorService;

    private MaxFlow(
        Graph graph,
        long source,
        long target,
        MaxFlowParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.graph = graph;
        this.source = source;
        this.target = target;
        this.parameters = parameters;
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
    }

    public static MaxFlow create(
        Graph graph,
        long sourceNode,
        long targetNode,
        MaxFlowParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        return new MaxFlow(
            graph,
            sourceNode,
            targetNode,
            parameters,
            executorService,
            progressTracker,
            terminationFlag
        );
    }

    public FlowResult compute() {
        var preflow = initPreflow(source);
        maximizeFlow(preflow, source, target);
        maximizeFlow(preflow, target, source);
        return preflow.flowGraph().createFlowResult(target);
    }

    private Preflow initPreflow(long source) {
        var excess = HugeDoubleArray.newArray(graph.nodeCount());
        excess.setAll(x -> 0D);
        var flowGraph = FlowGraph.create(graph);
        flowGraph.forEachRelationship(
            source, (s, t, relIdx, residualCapacity, isReverse) -> {
                flowGraph.push(relIdx, residualCapacity, isReverse);
                excess.set(t, residualCapacity);
                return true;
            }
        );
        var label = HugeLongArray.newArray(flowGraph.nodeCount());
        return new Preflow(flowGraph, excess, label);
    }

    private void maximizeFlow(Preflow preflow, long sourceNode, long targetNode) { //make non-static
        var flowGraph = preflow.flowGraph();
        var excess = preflow.excess();
        var label = preflow.label();

        var nodeCount = flowGraph.nodeCount();
        var edgeCount = flowGraph.edgeCount();

        var addedExcess = HugeAtomicDoubleArray.of(
            nodeCount,
            ParallelDoublePageCreator.passThrough(parameters.concurrency())
        ); //fixme
        var tempLabel = HugeLongArray.newArray(nodeCount);
        var isDiscovered = HugeAtomicBitSet.create(nodeCount);
        var workingSet = new AtomicWorkingSet(nodeCount);
        for (var nodeId = 0; nodeId < nodeCount; nodeId++) {
            if (excess.get(nodeId) > 0.0) {
                workingSet.push(nodeId);
            }
        }

        var workSinceLastGR = new AtomicLong(Long.MAX_VALUE);

        while (!workingSet.isEmpty()) {
            if (parameters.freq() * workSinceLastGR.doubleValue() > parameters.alpha() * nodeCount + edgeCount) {
                GlobalRelabeling.globalRelabeling(flowGraph, label, sourceNode, targetNode, parameters.concurrency());
                workSinceLastGR.set(0L);
            }
            Discharging.processWorkingSet(
                flowGraph,
                excess,
                label,
                tempLabel,
                addedExcess,
                isDiscovered,
                workingSet,
                targetNode,
                parameters.beta(),
                workSinceLastGR,
                parameters.concurrency()
            );
        }
    }
}
