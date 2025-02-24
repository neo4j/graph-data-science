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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeSerialObjectMergeSort;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

public class HDBScan extends Algorithm<Labels> {

    private final IdMap nodes;
    private final NodePropertyValues nodePropertyValues;
    private final Concurrency concurrency;
    private final long leafSize;
    private final TerminationFlag terminationFlag;
    private final int samples;
    private final long minClusterSize;

    public HDBScan(
        IdMap nodes,
        NodePropertyValues nodePropertyValues,
        Concurrency concurrency,
        long leafSize,
        int samples,
        long minClusterSize,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.nodes = nodes;
        this.nodePropertyValues = nodePropertyValues;
        this.concurrency = concurrency;
        this.leafSize = leafSize;
        this.samples = samples;
        this.minClusterSize = minClusterSize;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public Labels compute() {
        progressTracker.beginSubTask();
        var kdTree = buildKDTree();

        var nodeCount = nodes.nodeCount();
        var coreResult = computeCores(kdTree, nodeCount);

       // var mst = dualTreeMSTPhase(kdTree, coreResult);
        var mst = boruvka(kdTree,coreResult);

        var clusterHierarchy = createClusterHierarchy(mst);

        var condenseStep = new CondenseStep(nodeCount, progressTracker);
        var condensedTree = condenseStep.condense(clusterHierarchy, minClusterSize);

        var labellingStep = new LabellingStep(condensedTree, nodeCount, progressTracker);
        var labels= labellingStep.labels();
        progressTracker.endSubTask();

        return labels;
    }

    CoreResult computeCores(KdTree kdTree, long nodeCount) {
        HugeObjectArray<Neighbours> neighbours = HugeObjectArray.newArray(Neighbours.class, nodeCount);
        progressTracker.beginSubTask();
        ParallelUtil.parallelForEachNode(
            nodeCount, concurrency, terminationFlag,
            (nodeId) -> {
                neighbours.set(nodeId, kdTree.neighbours(nodeId, samples));
                progressTracker.logProgress();
            }
        );
        progressTracker.endSubTask();
        return new CoreResult(neighbours);
    }

    GeometricMSTResult dualTreeMSTPhase(KdTree kdTree, CoreResult coreResult) {
        var dualTreeMst = DualTreeMSTAlgorithm.create(
            nodePropertyValues,
            kdTree,
            coreResult,
            nodes.nodeCount()
        );
        return dualTreeMst.compute();
    }

    GeometricMSTResult boruvka(KdTree kdTree, CoreResult coreResult) {
        var boruvkaMST = BoruvkaMST.create(
            nodePropertyValues,
            kdTree,
            coreResult,
            nodes.nodeCount(),
            concurrency,
            progressTracker
        );
        return boruvkaMST.compute();
    }

    ClusterHierarchy createClusterHierarchy(GeometricMSTResult dualTreeMSTResult){
        var edges = dualTreeMSTResult.edges();
        HugeSerialObjectMergeSort.sort(Edge.class, edges, Edge::distance);
        return ClusterHierarchy.create(nodes.nodeCount(),edges,progressTracker);
    }

    KdTree buildKDTree() {
        var builder = new KdTreeBuilder(
            nodes,
            nodePropertyValues,
            concurrency.value(),
            leafSize,
            progressTracker
        );
        return builder.build();
    }
}
