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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeFunction;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.List;
import java.util.concurrent.ExecutorService;

public final class RandomWalkCompanion {

    public static RandomWalkSampler.CumulativeWeightSupplier cumulativeWeights(
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorsService,
        ProgressTracker progressTracker
    ) {
        return graph.hasRelationshipProperty()
            ? cumulativeWeightsFromProperty(graph, concurrency, executorsService, progressTracker)::get
            : graph::degree;
    }

    private static DegreeFunction cumulativeWeightsFromProperty(
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        return new DegreeCentrality(
            graph,
            executorService,
            concurrency,
            Orientation.NATURAL,
            true,
            ParallelUtil.DEFAULT_BATCH_SIZE,
            progressTracker
        ).compute().degreeFunction();

    }

    public static NextNodeSupplier nextNodeSupplier(Graph graph, List<Long> sourceNodes) {
        return sourceNodes.isEmpty()
            ? new NextNodeSupplier.GraphNodeSupplier(graph.nodeCount())
            : NextNodeSupplier.ListNodeSupplier.of(sourceNodes, graph);
    }

    private RandomWalkCompanion() {}
}
