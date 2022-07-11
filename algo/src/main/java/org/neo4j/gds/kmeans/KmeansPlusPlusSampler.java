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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;

public class KmeansPlusPlusSampler implements KmeansSampler {

    @Override
    public List<Long> sampleClusters(
        SplittableRandom random,
        NodePropertyValues nodePropertyValues,
        long nodeCount,
        int k
    ) {
        return null;
    }

    public void temp(
        SplittableRandom random,
        ClusterManager clusterManager,
        List<KmeansTask> tasks,
        int k,
        int concurrency,
        long nodeCount,
        HugeDoubleArray currentDistanceFromCenter,
        ExecutorService executorService
    ) {
        long firstId = random.nextLong();

        clusterManager.initialAssignCluster(0, firstId);
        for (int i = 1; i < k; ++i) {

            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(tasks)
                .executor(executorService)
                .run();

            double distanceFromClusterCentre = 0;
            for (KmeansTask task : tasks) {
                distanceFromClusterCentre += task.getDistanceFromClusterNormalized();
            }
            double x = random.nextDouble() * distanceFromClusterCentre;
            double curr = 0;
            long nextNode = -1;
            for (long nodeId = 0; nodeId < nodeCount; ++nodeId) { //something like that (smpling incorrect tho)
                if (curr > x) {

                    break;
                } else {
                    curr += currentDistanceFromCenter.get(nodeId);
                    nextNode = nodeId;
                }
            }
            clusterManager.initialAssignCluster(i, nextNode);
        }
        //nowe we have k clusters and distanceFromClusterAlso for each node closest communit in 0...k-2

        RunWithConcurrency.builder()  //now run one last time just to save  have the vest community in 0...k-1
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();
    }
}
