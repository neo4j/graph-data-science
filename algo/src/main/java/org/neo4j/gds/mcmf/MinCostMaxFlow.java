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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.maxflow.MaxFlow;
import org.neo4j.gds.maxflow.MaxFlowParameters;
import org.neo4j.gds.maxflow.SupplyAndDemandFactory;

import java.util.List;
import java.util.Optional;

public class MinCostMaxFlow extends Algorithm<CostFlowResult> {
    private final Graph graphOfFlows;
    private final Graph graphOfCosts;
    private CostFlowGraph costFlowGraph;
    private HugeDoubleArray excess;
    private final MCMFParameters parameters;

    static final double TOLERANCE = 1e-10;

    public MinCostMaxFlow(MCMFParameters parameters, ProgressTracker progressTracker, Graph graphOfFlows, Graph graphOfCosts) {
        super(progressTracker);
        this.graphOfFlows = graphOfFlows;
        this.graphOfCosts = graphOfCosts;
        this.parameters = parameters;
    }

    public static MinCostMaxFlow create(
        GraphStore graphStore,
        List<NodeLabel> nodeLabels,
        List<RelationshipType> relTypes,
        String flowProperty,
        String costProperty,
        MCMFParameters parameters,
        ProgressTracker progressTracker
    ) {
        var graphOfFlows = graphStore.getGraph(nodeLabels, relTypes, Optional.of(flowProperty));
        var graphOfCosts = graphStore.getGraph(nodeLabels, relTypes, Optional.of(costProperty));
        return new MinCostMaxFlow(parameters, progressTracker, graphOfFlows, graphOfCosts);
    }

    @Override
    public CostFlowResult compute() {
        //compute a maximal flow
        var maxFlow = new MaxFlow(graphOfFlows,
            new MaxFlowParameters(
                parameters.sourceNodes(),
                parameters.targetNodes(),
                parameters.concurrency(),
                0.5,
                true
            ),
            progressTracker,
            terminationFlag
        );
        initPreflow();
        maxFlow.computeMaxFlow(costFlowGraph, excess, HugeLongArray.newArray(costFlowGraph.nodeCount()));
        excess.setAll(x -> 0D);

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
        do {
            var start = System.currentTimeMillis();
            epsilon = Math.max(epsilon / parameters.alpha(), SMALLEST_ALLOWED_EPSILON);
            discharging.updateEpsilon(epsilon);
            initRefine(prize, workingQueue, inWorkingQueue);
            discharging.dischargeUntilDone();

        } while (epsilon > SMALLEST_ALLOWED_EPSILON);

        return costFlowGraph.createFlowResult();
    }

    private void initRefine(HugeDoubleArray prize, HugeLongArrayQueue workingQueue, BitSet inWorkingQueue) {
        assert(workingQueue.isEmpty());
        for(var node = 0; node < costFlowGraph.nodeCount(); node++) {
            costFlowGraph.forEachRelationship(node, (s, t, relIdx, residualCapacity, cost, isReverse) -> {
                //if reduced cost is negative then saturate
                var reducedCost = (cost + prize.get(s) - prize.get(t)); //todo: check this
                if (reducedCost < 0D) {
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

    private void initPreflow() {
        var supplyAndDemand = SupplyAndDemandFactory.create(graphOfFlows, parameters.sourceNodes(), parameters.targetNodes());
        costFlowGraph = new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supplyAndDemand.getLeft(),
            supplyAndDemand.getRight(),
            terminationFlag,
            parameters.concurrency()
        ).build();

        excess = HugeDoubleArray.newArray(costFlowGraph.nodeCount());
        excess.setAll(x -> 0D);
        costFlowGraph.forEachRelationship(
            costFlowGraph.superSource(), (s, t, relIdx, residualCapacity, isReverse) -> {
                costFlowGraph.push(relIdx, residualCapacity, isReverse);
                excess.set(t, residualCapacity);
                return true;
            }
        );
    }

}
