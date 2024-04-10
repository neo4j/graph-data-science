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
package org.neo4j.gds.triangle;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.ArrayList;

public class LocalClusteringCoefficientFactory<CONFIG extends LocalClusteringCoefficientBaseConfig> extends GraphAlgorithmFactory<LocalClusteringCoefficient, CONFIG> {

    @Override
    public String taskName() {
        return "LocalClusteringCoefficient";
    }

    public LocalClusteringCoefficient build(
        Graph graph,
        LocalClusteringCoefficientParameters parameters,
        ProgressTracker progressTracker
    ) {
        return new LocalClusteringCoefficient(
            graph,
            parameters.concurrency().value(),
            parameters.maxDegree(),
            parameters.seedProperty(),
            progressTracker
        );
    }

    @Override
    public LocalClusteringCoefficient build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return build(graph, configuration.toParameters(), progressTracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return new LocalClusteringCoefficientMemoryEstimateDefinition(configuration.seedProperty()).memoryEstimation();
    }


    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph, config.seedProperty());
    }

    public Task progressTask(Graph graph, @Nullable String seedProperty) {
        var tasks = new ArrayList<Task>();
        if (seedProperty == null) {
            tasks.add(IntersectingTriangleCountFactory.triangleCountProgressTask(graph));
        }
        tasks.add(Tasks.leaf("Calculate Local Clustering Coefficient", graph.nodeCount()));
        return Tasks.task(taskName(), tasks);
    }
}
