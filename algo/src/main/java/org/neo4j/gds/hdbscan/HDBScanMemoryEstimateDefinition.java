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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

public class HDBScanMemoryEstimateDefinition implements MemoryEstimateDefinition {
    private static final int DIM_SIZE = 10;

    private final HDBScanParameters parameters;

    public HDBScanMemoryEstimateDefinition(HDBScanParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(HDBScan.class)
            .add("kd-tree creation", kdTree())
            .add("boruvka", boruvka())
            .add("cluster hierarchy", clusterHierarchyPhase())
            .add("condensing phase", condensingPhase())
            .add("labelling phase", labellingPhase())
            .build();
    }

    private MemoryEstimation clusterHierarchyPhase() {

        var unionFind = MemoryEstimations.builder(ClusterHierarchyUnionFind.class)
            .perNode("parent", (nodeCount) -> HugeLongArray.memoryEstimation(2 * nodeCount))
            .build();

        var clusterHierarchy = MemoryEstimations.builder(ClusterHierarchy.class)
            .perNode("left", HugeLongArray::memoryEstimation)
            .perNode("right", HugeLongArray::memoryEstimation)
            .perNode("size", HugeLongArray::memoryEstimation)
            .perNode("lambda", HugeDoubleArray::memoryEstimation)
            .add("union find", unionFind);

        return clusterHierarchy.build();

    }

    private MemoryEstimation labellingPhase() {

        return MemoryEstimations.builder(LabellingStep.class)
            .perNode("stabilities", HugeDoubleArray::memoryEstimation)
            .perNode("stability Sums", HugeDoubleArray::memoryEstimation)
            .perNode("selected bitset", Estimate::sizeOfBitset)
            .perNode("tree labels", HugeLongArray::memoryEstimation)
            .perNode("node labels", HugeLongArray::memoryEstimation)
            .add("labels", MemoryEstimations.builder(Labels.class).build())
            .build();
    }

    private MemoryEstimation condensingPhase() {
        return MemoryEstimations.builder(CondenseStep.class)
            .perNode("parent", (nodeCount) -> HugeLongArray.memoryEstimation(2 * nodeCount))
            .perNode("lambda", (nodeCount) -> HugeDoubleArray.memoryEstimation(2 * nodeCount))
            .perNode("size", HugeLongArray::memoryEstimation)
            .perNode("relabel", HugeLongArray::memoryEstimation)
            .perNode("bfs queue", HugeLongArray::memoryEstimation)
            .add("condensed tree", MemoryEstimations.builder(CondensedTree.class).build())
            .build();

    }

    private MemoryEstimation boruvka() {

        var distanceTracker = MemoryEstimations.builder(ClosestDistanceTracker.class)
            .perNode("inside point", HugeLongArray::memoryEstimation)
            .perNode("outside point", HugeLongArray::memoryEstimation)
            .perNode("best component distance", HugeDoubleArray::memoryEstimation)
            .build();

        long neighbourObjectSize = Estimate.sizeOfInstance(Neighbour.class);

        var samples = parameters.samples();
        var cores = MemoryEstimations.builder()
            .perNode("nearest nodes", (nodeCount) -> Estimate.sizeOfArray(samples, neighbourObjectSize) * nodeCount)
            .build();


        var edgeSize = Estimate.sizeOfInstance(Edge.class);
        var boruvka = MemoryEstimations.builder(BoruvkaMST.class)
            .add("distance tracker", distanceTracker)
            .perNode("cores", HugeDoubleArray::memoryEstimation)
            .add("union find", HugeAtomicDisjointSetStruct.memoryEstimation(false))
            .add(cores)
            .perNode("MST relationships", nodeCount -> HugeObjectArray.memoryEstimation(nodeCount - 1, edgeSize))
            .add("result", MemoryEstimations.builder(GeometricMSTResult.class).build());

        return boruvka.build();
    }

    private MemoryEstimation kdTree() {

        var kdTree = MemoryEstimations.builder(KdTree.class)
            .perNode("ids", HugeLongArray::memoryEstimation)
            .perGraphDimension(
                "foo", ((graphDimensions, concurrency) -> {
                    var estimatedNumberOfNodes = estimatedNumberOfNodes(
                        graphDimensions.nodeCount(),
                        parameters.leafSize()
                    );
                    var minArray = Estimate.sizeOfDoubleArray(DIM_SIZE);
                    var maxArray = Estimate.sizeOfDoubleArray(DIM_SIZE);
                    var aabbSize = Estimate.sizeOfInstance(AABB.class) + minArray + maxArray;
                    var kdNodeSize = Estimate.sizeOfInstance(KdNode.class) + aabbSize;
                    return MemoryRange.of(kdNodeSize * estimatedNumberOfNodes);
                })
            );

        return kdTree.build();
    }

    static long estimatedNumberOfNodes(long nodeCount, long leafSize) {
        var levels = (long) Math.ceil(Math.log(nodeCount / (1.0 * leafSize)) / Math.log(2));

        var approximateKdNodes = (long) Math.pow(2, levels + 1) - 1;

        return Math.min(2 * nodeCount - 1, approximateKdNodes);
    }
}
