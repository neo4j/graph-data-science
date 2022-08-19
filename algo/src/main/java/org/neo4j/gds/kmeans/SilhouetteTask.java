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

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public abstract class SilhouetteTask implements Runnable {

    final HugeIntArray communities;
    final HugeDoubleArray silhouette;

    final double[] clusterDistance;
    final ProgressTracker progressTracker;
    final Partition partition;

    final long[] nodesInCluster;

    final int k;
    final int dimensions;

    double averageSilhouette;
    final NodePropertyValues nodePropertyValues;

    abstract double distance(long nodeA, long nodeB);


    SilhouetteTask(
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        HugeDoubleArray silhouette,
        int k,
        int dimensions,
        long[] nodesInCluster,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.nodePropertyValues = nodePropertyValues;
        this.communities = communities;
        this.k = k;
        this.dimensions = dimensions;
        this.partition = partition;
        this.progressTracker = progressTracker;
        this.silhouette = silhouette;
        this.nodesInCluster = nodesInCluster;
        this.clusterDistance = new double[k];
        this.averageSilhouette = 0d;

    }

    @Override
    public void run() {
        long nodeCount = communities.size();
        var startNode = partition.startNode();
        var endNode = startNode + partition.nodeCount();
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            int clusterId = communities.get(nodeId);
            if (nodesInCluster[clusterId] == 1) {
                silhouette.set(nodeId, 0);
            } else {
                for (int cluster = 0; cluster < k; ++cluster) {
                    clusterDistance[cluster] = 0;
                }
                for (long oNodeId = 0; oNodeId < nodeCount; ++oNodeId) {
                    if (oNodeId == nodeId) {
                        continue;
                    }
                    double euclidean = distance(nodeId, oNodeId);
                    int oClusterId = communities.get(oNodeId);
                    clusterDistance[oClusterId] += euclidean;
                }
                double bi = Double.MAX_VALUE;
                for (int cluster = 0; cluster < k; ++cluster) {
                    if (clusterId == cluster) continue;
                    bi = Math.min(
                        bi,
                        clusterDistance[cluster] / ((double) nodesInCluster[cluster])
                    );
                }
                double ai = clusterDistance[clusterId] / ((double) (nodesInCluster[clusterId] - 1));
                double nodeSilhouette = (bi - ai) / Math.max(ai, bi);
                silhouette.set(nodeId, nodeSilhouette);
                averageSilhouette += nodeSilhouette;
                progressTracker.logProgress();
            }
        }
    }

    public double getAverageSilhouette() {return averageSilhouette / (double) (communities.size());}

    public static SilhouetteTask createTask(
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        HugeDoubleArray silhouette,
        int k,
        int dimensions,
        long[] nodesInCluster,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        if (nodePropertyValues.valueType() == ValueType.FLOAT_ARRAY) {
            return new FloatSilhouetteTask(
                nodePropertyValues,
                communities,
                silhouette,
                k,
                dimensions,
                nodesInCluster,
                partition,
                progressTracker
            );
        }
        return new DoubleSilhouetteTask(
            nodePropertyValues,
            communities,
            silhouette,
            k,
            dimensions,
            nodesInCluster,
            partition,
            progressTracker
        );

    }
}

class DoubleSilhouetteTask extends SilhouetteTask {

    DoubleSilhouetteTask(
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        HugeDoubleArray silhouette,
        int k,
        int dimensions,
        long[] nodesInCluster,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        super(
            nodePropertyValues,
            communities,
            silhouette,
            k,
            dimensions,
            nodesInCluster,
            partition,
            progressTracker
        );
    }


    @Override
    double distance(long nodeA, long nodeB) {
        double[] left = nodePropertyValues.doubleArrayValue(nodeA);
        double[] right = nodePropertyValues.doubleArrayValue(nodeB);
        return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));
    }
}

class FloatSilhouetteTask extends SilhouetteTask {

    FloatSilhouetteTask(
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        HugeDoubleArray silhouette,
        int k,
        int dimensions,
        long[] nodesInCluster,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        super(
            nodePropertyValues,
            communities,
            silhouette,
            k,
            dimensions,
            nodesInCluster,
            partition,
            progressTracker
        );
    }

    @Override
    double distance(long nodeA, long nodeB) {
        float[] left = nodePropertyValues.floatArrayValue(nodeA);
        float[] right = nodePropertyValues.floatArrayValue(nodeB);
        return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));

    }
}


