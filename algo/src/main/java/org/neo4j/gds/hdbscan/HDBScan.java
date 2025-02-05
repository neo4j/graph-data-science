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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Arrays;

public class HDBScan extends Algorithm<Void> {

    private final IdMap nodes;
    private final NodePropertyValues nodePropertyValues;
    private final Concurrency concurrency;
    private final long leafSize;
    private final TerminationFlag terminationFlag;
    private final int k;

    protected HDBScan(
        IdMap nodes,
        NodePropertyValues nodePropertyValues,
        Concurrency concurrency,
        long leafSize,
        int k,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
        ) {
        super(progressTracker);
        this.nodes = nodes;
        this.nodePropertyValues = nodePropertyValues;
        this.concurrency = concurrency;
        this.leafSize = leafSize;
        this.k = k;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public Void compute() {
        var kdTree =  buildKDTree();
        HugeDoubleArray coreValues = HugeDoubleArray.newArray(nodes.nodeCount());

        ParallelUtil.parallelForEachNode(nodes.nodeCount(),concurrency, terminationFlag,
            (nodeId) ->{
                    var coreValue =  Arrays.stream(kdTree.neighbours(nodeId,k)).map(Neighbour::distance).max(Double::compareTo).orElse(0D);
                        coreValues.set(nodeId,coreValue);
            });

        var duaTreeMST = dualTreeMSTPhase(kdTree,coreValues);
        return null;
    }

    HugeObjectArray<Edge> dualTreeMSTPhase(KdTree kdTree, HugeDoubleArray coreValues){
            var dualTreeMst =new DualTreeMSTAlgorithm(
                nodePropertyValues,
                kdTree,coreValues,
                nodes.nodeCount()
            );
            return dualTreeMst.compute().edges();
    }
    KdTree buildKDTree(){
        var builder =new KdTreeBuilder(nodes,nodePropertyValues,concurrency.value(),leafSize);
        return  builder.build();
    }
}
