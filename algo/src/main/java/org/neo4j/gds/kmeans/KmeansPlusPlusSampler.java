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


import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;

public class KmeansPlusPlusSampler extends KmeansSampler {

    private List<KmeansTask> tasks;
    private int concurrency;
    private HugeDoubleArray distanceFromClosestCentroid;
    private ExecutorService executorService;

    private NodePropertyValues nodePropertyValues;


    public KmeansPlusPlusSampler(
        SplittableRandom random,
        ClusterManager clusterManager,
        long nodeCount,
        int k,
        NodePropertyValues nodePropertyValues,
        HugeDoubleArray distanceFromClosestCentroid,
        int concurrency,
        ExecutorService executorService,
        List<KmeansTask> tasks
    ) {
        super(random, clusterManager, nodeCount, k);
        this.nodePropertyValues = nodePropertyValues;
        this.distanceFromClosestCentroid = distanceFromClosestCentroid;
        this.executorService = executorService;
        this.tasks = tasks;
        this.concurrency = concurrency;
    }


    @Override
    public void performInitialSampling() {
        long firstId = random.nextLong(nodeCount);

        BitSet bitSet = new BitSet(nodeCount);
        clusterManager.initialAssignCluster(firstId);
        distanceFromClosestCentroid.set(firstId, -1);

        for (int i = 1; i < k; ++i) {

            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(tasks)
                .executor(executorService)
                .run();

            double squaredDistance = 0;
            for (KmeansTask task : tasks) {
                squaredDistance += task.getSquaredDistance();
            }
            long nextNode = -1;

            //This is fail-case in case of overflow
            if (!(Double.isInfinite(squaredDistance) || squaredDistance <= 0)) {

                double x = random.nextDouble() * squaredDistance;
                double curr = 0;

                for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
                    double distanceFromCentroid = distanceFromClosestCentroid.get(nodeId);
                    if (distanceFromCentroid <= -1) {
                        continue;
                    }
                    curr += distanceFromCentroid * distanceFromCentroid;

                    if (x <= curr) {
                        nextNode = nodeId;
                        break;
                    }

                }
            }
            if (nextNode == -1) {
                nextNode = random.nextLong(nodeCount);
                while (bitSet.get(nextNode)) {
                    nextNode = random.nextLong(nodeCount);
                }
            }
            bitSet.set(nextNode);
            clusterManager.initialAssignCluster(nextNode);
            distanceFromClosestCentroid.set(nextNode, -(i + 1));
        }
        //nowe we have k clusters and distanceFromClusterAlso for each node closest communit in 0...k-2

        RunWithConcurrency.builder()  //now run one last time just to save  have the vest community in 0...k-1
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();

        for (KmeansTask task : tasks) {
            task.switchToPhase(TaskPhase.ITERATION);
        }

    }
}

