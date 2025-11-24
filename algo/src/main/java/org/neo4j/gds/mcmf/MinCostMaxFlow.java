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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.maxflow.MaxFlowPhase;
import org.neo4j.gds.maxflow.SupplyAndDemandFactory;
import org.neo4j.gds.termination.TerminationFlag;

import static org.neo4j.gds.mcmf.MinCostFunctions.TOLERANCE;
import static org.neo4j.gds.mcmf.MinCostFunctions.isAdmissible;

public final class MinCostMaxFlow extends Algorithm<CostFlowResult> {
    private final Graph graphOfFlows;
    private final Graph graphOfCosts;
    private final MCMFParameters parameters;


    public MinCostMaxFlow(
        Graph graphOfFlows,
        Graph graphOfCosts,
        MCMFParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.graphOfFlows = graphOfFlows;
        this.graphOfCosts = graphOfCosts;
        this.parameters = parameters;
        this.terminationFlag = terminationFlag;
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

        var SMALLEST_ALLOWED_EPSILON = 1D/costFlowGraph.nodeCount();

        var epsilon = costFlowGraph.maximalUnitCost();
        var discharging = new CostDischarging(
            costFlowGraph,
            excess,
            prize,
            workingQueue,
            inWorkingQueue,
            epsilon,
            new GlobalRelabelling(costFlowGraph, excess, prize),
            parameters.freq()
        );
        progressTracker.beginSubTask();;
        do {
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

    void computeMaxFlow(CostFlowGraph flowGraph,HugeDoubleArray excess){
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
                if (isAdmissible(reducedCost)) {
                    var delta = residualCapacity;
                    costFlowGraph.push(relIdx, delta, isReverse);
                    excess.addTo(s, -delta);
                    excess.addTo(t, delta);
                }
                return true;
            });
        }
        for(var node = 0; node < costFlowGraph.nodeCount(); node++){
            if (excess.get(node) > TOLERANCE) {
                workingQueue.add(node);
                inWorkingQueue.set(node);
            }
        }
    }
    private CostFlowGraph createFlowGraph(){
        var supplyAndDemand = SupplyAndDemandFactory.create(
            graphOfFlows,
            parameters.maxFlowParameters().sourceNodes(),
            parameters.maxFlowParameters().targetNodes()
        );
        return new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supplyAndDemand.getLeft(),
            supplyAndDemand.getRight(),
            terminationFlag,
            parameters.concurrency()
        ).build();
    }

}
