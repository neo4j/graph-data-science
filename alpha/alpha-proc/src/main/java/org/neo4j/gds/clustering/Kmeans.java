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
package org.neo4j.gds.clustering;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.SplittableRandom;

public class Kmeans extends Algorithm<HugeLongArray> {
    private final HugeLongArray inCommunity;
    private final Graph graph;
    private final KmeansBaseConfig config;
    private final KmeansContext context;
    private final SplittableRandom splittableRandom;
    private final HugeObjectArray<double[]> nodeProperties;

    public static Kmeans createKmeans(Graph graph, KmeansBaseConfig config, KmeansContext context) {
        String nodeWeightProperty = config.nodeWeightProperty();
        NodeProperties nodePropertiesAll = graph.nodeProperties(nodeWeightProperty);
        HugeObjectArray<double[]> nodeProperties = HugeObjectArray.newArray(double[].class, graph.nodeCount());
        graph.forEachNode(nodeId -> {
            nodeProperties.set(nodeId, nodePropertiesAll.doubleArrayValue(nodeId));
            return true;
        });
        return new Kmeans(
            context.progressTracker(),
            graph,
            config,
            context,
            nodeProperties,
            getSplittableRandom(config.randomSeed())
        );
    }

    Kmeans(
        ProgressTracker progressTracker,
        Graph graph,
        KmeansBaseConfig config,
        KmeansContext context,
        HugeObjectArray<double[]> nodeProperties,
        SplittableRandom splittableRandom
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.context = context;
        this.splittableRandom = splittableRandom;
        this.inCommunity = HugeLongArray.newArray(graph.nodeCount());
        this.nodeProperties = nodeProperties;
    }

    @Override
    public HugeLongArray compute() {
        return null;
    }

    @Override
    public void release() {

    }

    @NotNull
    private static SplittableRandom getSplittableRandom(Optional<Long> randomSeed) {
        return randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
    }
}
