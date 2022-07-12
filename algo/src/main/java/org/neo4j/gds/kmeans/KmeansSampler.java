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
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;

public abstract class KmeansSampler {

    final SplittableRandom random;
    final int k;
    final long nodeCount;
    final ClusterManager clusterManager;


    public abstract void performInitialSampling();

    public KmeansSampler(
        SplittableRandom random,
        ClusterManager clusterManager,
        long nodeCount,
        int k
    ) {
        this.random = random;
        this.nodeCount = nodeCount;
        this.clusterManager = clusterManager;
        this.k = k;
    }

    public static KmeansSampler createSampler(
        SamplerType samplerType,
        SplittableRandom random,
        NodePropertyValues nodePropertyValues,
        ClusterManager clusterManager,
        long nodeCount,
        int k,
        int concurrency,
        HugeDoubleArray distanceFromCenter,
        ExecutorService executorService,
        List<KmeansTask> tasks
    ) {
        if (samplerType == SamplerType.UNIFORM) {
            return new KmeansUniformSampler(random, clusterManager, nodeCount, k);
        } else
            return new KmeansPlusPlusSampler(
                random,
                clusterManager,
                nodeCount,
                k,
                nodePropertyValues,
                distanceFromCenter,
                concurrency,
                executorService,
                tasks
            );
    }

}

enum SamplerType {
    UNIFORM, KMEANSPP;
}
