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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

public final class MaxFlow extends Algorithm<FlowResult> {
    private final Graph graph;
    private final MaxFlowParameters parameters;

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
        var flowGraph= createFlowGraph();
        var excess = HugeDoubleArray.newArray(flowGraph.nodeCount());
        var maxFlowPhase  = new MaxFlowPhase(flowGraph,excess,parameters,progressTracker,terminationFlag);
        maxFlowPhase.computeMaxFlow();
        progressTracker.endSubTask();
        return flowGraph.createFlowResult();
    }

    private FlowGraph createFlowGraph(){
        var supplyAndDemand = SupplyAndDemandFactory.create(graph, parameters.sourceNodes(), parameters.targetNodes());
        return new FlowGraphBuilder(
            graph,
            supplyAndDemand.getLeft(),
            supplyAndDemand.getRight(),
            terminationFlag,
            parameters.concurrency()
        ).build();
    }






}
