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

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.maxflow.DefaultRelationships;
import org.neo4j.gds.maxflow.FlowGraphBuilder;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class CostFlowGraphBuilder extends FlowGraphBuilder {

    private HugeDoubleArray cost;
    private final Graph costGraph;
    private  boolean allowNegativeCosts = false;

    public CostFlowGraphBuilder(
        Graph capacityGraph,
        Graph costGraph,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        TerminationFlag terminationFlag,
        Concurrency concurrency
    ) {
        super(capacityGraph,supply, demand, terminationFlag, concurrency);
        this.costGraph = costGraph;
    }

    @TestOnly
    CostFlowGraphBuilder withNegativeCosts(){
        allowNegativeCosts = true;
        return this;
    }

    private  void setUpCosts(){
        this.cost = HugeDoubleArray.newArray(flow.size());
        AtomicLong nodeId = new AtomicLong();
        long oldNodeCount = costGraph.nodeCount();

        Function<MutableLong, RelationshipWithPropertyConsumer> consumerProducer = (relIdx)-> (s, t, w) -> {
            if (w < 0 && !allowNegativeCosts){
                throw new IllegalArgumentException("Negative costs are not allowed");
            }
            cost.set(relIdx.getAndIncrement(), w);
            return true;
        };

        var tasks = ParallelUtil.tasks(
            concurrency,
            () -> () -> {
                var costGraphCopy = costGraph.concurrentCopy();
                long v;
                while ((v = nodeId.getAndIncrement()) < oldNodeCount) {
                    var relIdx = new MutableLong(outRelationshipIndexOffset.get(v));
                    var consumer = consumerProducer.apply(relIdx);
                    costGraphCopy.forEachRelationship(v, 0D, consumer);
                }
            }
        );

        RunWithConcurrency.builder()
            .tasks(tasks)
            .concurrency(concurrency)
            .run();
    }
    public CostFlowGraph build() {

        setUpCapacities();
        setUpCosts();
        return new CostFlowGraph(
            capacityGraph,
            outRelationshipIndexOffset,
            new CostRelationships(new DefaultRelationships(originalCapacity,flow,nodeConstraintsIdMap),cost,capacityGraph.relationshipCount()),
            reverseAdjacency,
            reverseRelationshipMap,
            reverseRelationshipIndexOffset,
            supply,
            demand
        );
    }
}
