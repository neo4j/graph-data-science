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
package org.neo4j.gds.traversal;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.ImmutableDegreeCentralityConfig;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.concurrent.ExecutorService;

public class RandomWalkCompanion {

    public static RandomWalkSampler.CumulativeWeightSupplier cumulativeWeights(
        Graph graph,
        RandomWalkBaseConfig config,
        ExecutorService executorsService,
        ProgressTracker progressTracker
    ) {
        return graph.hasRelationshipProperty()
            ? cumulativeWeights(graph, config.concurrency(), executorsService, progressTracker)::get
            : graph::degree;
    }

    private static DegreeCentrality.DegreeFunction cumulativeWeights(
        Graph graph,
        int concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        var degreeCentralityConfig = ImmutableDegreeCentralityConfig.builder()
            .concurrency(concurrency)
            // DegreeCentrality internally decides its computation on the config. The actual property key is not relevant
            .relationshipWeightProperty("DUMMY")
            .build();

        return new DegreeCentrality(
            graph,
            executorService,
            degreeCentralityConfig,
            progressTracker
        ).compute();

    }
    public static  NextNodeSupplier nextNodeSupplier(Graph graph, RandomWalkBaseConfig config){
      return   config.sourceNodes() == null || config.sourceNodes().isEmpty()
            ? new NextNodeSupplier.GraphNodeSupplier(graph.nodeCount())
            : NextNodeSupplier.ListNodeSupplier.of(config, graph);
    }
}
