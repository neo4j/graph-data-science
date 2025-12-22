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
package org.neo4j.gds.mcmf;

import com.carrotsearch.hppc.BitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.maxflow.IgnoreNodeConstraints;
import org.neo4j.gds.maxflow.MaxFlowPhase;
import org.neo4j.gds.maxflow.NodeConstraintsFromPropertyIdMap;
import org.neo4j.gds.maxflow.NodeConstraintsIdMap;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.maxflow.SupplyAndDemandFactory;
import org.neo4j.gds.termination.TerminationFlag;

import static org.neo4j.gds.maxflow.MaxFlowFunctions.TOLERANCE;
import static org.neo4j.gds.mcmf.MinCostFunctions.isAdmissible;
import static org.neo4j.gds.mcmf.MinCostFunctions.isResidualEdge;

public final class MinCostMaxFlow extends Algorithm<CostFlowResult> {
    private final Graph graphOfFlows;
    private final Graph graphOfCosts;
    private final MCMFParameters parameters;

    private final NodeConstraintsIdMap constraints;
    private final Pair<NodeWithValue[],NodeWithValue[]> supplyAndDemand;

    public static MinCostMaxFlow create(
        Graph graphOfFlows,
        Graph graphOfCosts,
        MCMFParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        NodeConstraintsIdMap nodeConstraints;
        var maxFlowParams = parameters.maxFlowParameters();
        Pair<NodeWithValue[], NodeWithValue[]> supplyAndDemand;
        if (maxFlowParams.nodeCapacityProperty().isEmpty()) {
            nodeConstraints = new IgnoreNodeConstraints();
            supplyAndDemand = SupplyAndDemandFactory.create(graphOfFlows, maxFlowParams.sourceNodes(), maxFlowParams.targetNodes());
        } else {
            var nodePropertyValues = graphOfFlows.nodeProperties(maxFlowParams.nodeCapacityProperty().get());
            nodeConstraints = NodeConstraintsFromPropertyIdMap.create(
                graphOfFlows,
                graphOfFlows.relationshipCount(),
                nodePropertyValues,
                maxFlowParams.sourceNodes(),
                maxFlowParams.targetNodes()
            );
            supplyAndDemand = SupplyAndDemandFactory.create(
                graphOfFlows,
                nodePropertyValues,
                maxFlowParams.sourceNodes(),
                maxFlowParams.targetNodes()
            );
        }
            return new MinCostMaxFlow(
                graphOfFlows,
                graphOfCosts,
                parameters,
                progressTracker,
                terminationFlag,
                nodeConstraints,
                supplyAndDemand
            );
    }

    public MinCostMaxFlow(
        Graph graphOfFlows,
        Graph graphOfCosts,
        MCMFParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        NodeConstraintsIdMap nodeConstraintsIdMap,
        Pair<NodeWithValue[],NodeWithValue[]> supplyAndDemand
    ) {
        super(progressTracker);
        this.graphOfFlows = graphOfFlows;
        this.graphOfCosts = graphOfCosts;
        this.parameters = parameters;
        this.constraints = nodeConstraintsIdMap;
        this.terminationFlag = terminationFlag;
        this.supplyAndDemand = supplyAndDemand;
    }

    @Override
    public CostFlowResult compute() {

        progressTracker.beginSubTask();
        //create flowgraph
        var costFlowGraph = createFlowGraph();
        //
        var  excess = HugeDoubleArray.newArray(costFlowGraph.nodeCount());
        computeMaxFlow(costFlowGraph,excess);
        var prize = HugeDoubleArray.newArray(costFlowGraph.nodeCount());
        prize.setAll(x -> 0D);
        var workingQueue = HugeLongArrayQueue.newQueue(costFlowGraph.nodeCount());
        var inWorkingQueue = new BitSet(costFlowGraph.nodeCount());
       // costFlowGraph.scaleCosts();
        var SMALLEST_ALLOWED_EPSILON = Math.max(TOLERANCE,1.0/costFlowGraph.nodeCount());
        var epsilon = costFlowGraph.maximalUnitCost();
        var discharging = new CostDischarging(
            costFlowGraph,
            excess,
            prize,
            workingQueue,
            inWorkingQueue,
            epsilon,
            new GlobalRelabelling(costFlowGraph, excess, prize,terminationFlag),
            parameters.freq(),
            terminationFlag
        );

        progressTracker.beginSubTask();
        do {
            terminationFlag.assertRunning();
            progressTracker.beginSubTask();
            epsilon = Math.max(epsilon / parameters.alpha(), SMALLEST_ALLOWED_EPSILON);
            discharging.updateEpsilon(epsilon);
            initRefine(costFlowGraph,excess,prize, workingQueue, inWorkingQueue);
            discharging.dischargeUntilDone();
            progressTracker.endSubTask();

        } while (epsilon > SMALLEST_ALLOWED_EPSILON);
        progressTracker.endSubTask();
        progressTracker.endSubTask();

        return costFlowGraph.createFlowResult();

    }

    private void computeMaxFlow(CostFlowGraph flowGraph, HugeDoubleArray excess){
        progressTracker.beginSubTask();

        var maxFlow = new MaxFlowPhase(
            flowGraph,
            excess,
            parameters.maxFlowParameters(),
            progressTracker,
            terminationFlag
        );
        maxFlow.computeMaxFlow();
        progressTracker.endSubTask();
    }

    private void initRefine(
        CostFlowGraph costFlowGraph,
        HugeDoubleArray excess,
        HugeDoubleArray prize,
        HugeLongArrayQueue workingQueue,
        BitSet inWorkingQueue
    ) {
        assert(workingQueue.isEmpty());
        excess.setAll( v->0);
        for(var node = 0; node < costFlowGraph.nodeCount(); node++) {
            costFlowGraph.forEachRelationship(node, (s, t, relIdx, residualCapacity, cost, isReverse) -> {
                //if reduced cost is negative then saturate
                var reducedCost = MinCostFunctions.reducedCost(cost, prize.get(s), prize.get(t));
                if (isAdmissible(reducedCost) && isResidualEdge(residualCapacity)) {
                    var delta = residualCapacity;
                    costFlowGraph.push(relIdx, delta, isReverse);
                    excess.addTo(s, -delta);
                    excess.addTo(t, delta);
                }
                return true;
            });
        }
        for(var node = 0; node < costFlowGraph.nodeCount(); node++){
            if (MinCostFunctions.treatAsPositive(excess.get(node))) {
                workingQueue.add(node);
                inWorkingQueue.set(node);
            }
        }
    }
    private CostFlowGraph createFlowGraph(){

        return new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supplyAndDemand.getLeft(),
            supplyAndDemand.getRight(),
            terminationFlag,
            parameters.concurrency(),
            constraints
        ).build();
    }

}
