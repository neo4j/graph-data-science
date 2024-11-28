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
package org.neo4j.gds.pricesteiner;

import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;

public class PrizeSteinerTreeMemoryEstimateDefinition implements MemoryEstimateDefinition {

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(PCSTFast.class)
            .add(growthSpecificMemory())
            .add(treeProducerSpecificMemory())
            .add(pruningSpecificMemory())
            .build();
    }

    private MemoryEstimation  growthSpecificMemory(){
        return MemoryEstimations.builder(GrowthPhase.class)
            .add("cluster structure", clusterStructureSpecificMemory())
            .add("edge queue", edgeQueueSpecificMemory())
            .add("cluster queue", clusterQueueSpecificMemory())
            .rangePerGraphDimension("edge save", ((graphDimensions, concurrency) -> {
                long  edges = graphDimensions.relCountUpperBound();
                var edgePartsSize = HugeLongArray.memoryEstimation(edges);
                var edgeWeightsSize=  HugeDoubleArray.memoryEstimation(edges/2);
                return MemoryRange.of(edgePartsSize +edgeWeightsSize);
            }))
            .perNode("tree edges", HugeLongArray::memoryEstimation)
            .build();
    }

    private MemoryEstimation  clusterStructureSpecificMemory(){
        return MemoryEstimations.builder(ClusterStructure.class)
            .rangePerNode("parent",   nc-> MemoryRange.of(HugeLongArray.memoryEstimation(2*nc)))
            .rangePerNode("initMoatLeft",   nc-> MemoryRange.of(HugeDoubleArray.memoryEstimation(2*nc)))
            .rangePerNode("skippedParent",   nc-> MemoryRange.of(HugeDoubleArray.memoryEstimation(2*nc)))
            .rangePerNode("moat",   nc-> MemoryRange.of(HugeDoubleArray.memoryEstimation(2*nc)))
            .perNode("left", HugeLongArray::memoryEstimation)
            .perNode("right", HugeLongArray::memoryEstimation)
            .perNode("active", Estimate::sizeOfBitset)
            .perNode("stack", HugeLongArray::memoryEstimation)
            .add("clusterActivity",
                MemoryEstimations.builder(ClusterActivity.class)
                    .rangePerNode("active", nc -> MemoryRange.of(Estimate.sizeOfBitset(2*nc)))
                    .rangePerNode("relevantTime",   nc-> MemoryRange.of(HugeDoubleArray.memoryEstimation(2*nc)))
                    .build()
            )
            .build();
    }

    private MemoryEstimation  edgeQueueSpecificMemory() {
        return MemoryEstimations.builder(EdgeEventsQueue.class)
            .add("queue", HugeLongPriorityQueue.memoryEstimation())
            .perGraphDimension("pairing heap elements", (graphDimensions, concurrency) -> MemoryRange.of(graphDimensions.relCountUpperBound()*sizeOfInstance(PairingHeapElement.class)
            ))
            .perNode("merging array", Estimate::sizeOfLongArrayList) //this may need some fixing
            .rangePerNode("foo",
                nc-> MemoryRange.of(HugeObjectArray.memoryEstimation(2*nc, Estimate.sizeOfInstance(PairingHeap.class)))
            )
            .build();

    }

    private MemoryEstimation  clusterQueueSpecificMemory() {
        return MemoryEstimations.builder(ClusterEventsPriorityQueue.class)
            .add(HugeLongPriorityQueue.memoryEstimation())
            .build();

    }
        private MemoryEstimation treeProducerSpecificMemory(){
        return MemoryEstimations.builder(TreeProducer.class)
            .perNode("degree",HugeLongArray::memoryEstimation)
            .rangePerGraphDimension("tree", (graphDimensions, concurrency) -> {
                 var  treeDimensions =  ImmutableGraphDimensions
                    .builder()
                    .nodeCount(graphDimensions.nodeCount())
                    .relCountUpperBound(graphDimensions.nodeCount()-1)
                     .build();

                // Tree Producer creates a graph!
                RelationshipProjections relationshipProjections = ImmutableRelationshipProjections.builder()
                    .putProjection(
                        RelationshipType.of("PLACEHOLDER"),
                        RelationshipProjection.builder()
                            .type("PLACEHOLDER")
                            .orientation(Orientation.UNDIRECTED)
                            .addProperty("irrelevant", "irrelevant", DefaultValue.of(0.0))
                            .build()
                    )
                    .build();

                long maxGraphSize = CSRGraphStoreFactory
                    .getMemoryEstimation(NodeProjections.all(), relationshipProjections, false)
                    .estimate(treeDimensions, concurrency)
                    .memoryUsage()
                    .max;

                return MemoryRange.of(1L, maxGraphSize); // rough estimate of graph size
            })
            .build();

    }

    private  MemoryEstimation pruningSpecificMemory(){
        return MemoryEstimations.builder(StrongPruning.class)
            .perNode("parent",HugeLongArray::memoryEstimation)
            .perNode("parentCost", HugeDoubleArray::memoryEstimation)
            .perNode("queue",HugeLongArray::memoryEstimation)
            .perNode("dynamic programming array", HugeDoubleArray::memoryEstimation)
            .build();
    }
}
