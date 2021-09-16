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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.ArrayList;

public class LocalClusteringCoefficientFactory<CONFIG extends LocalClusteringCoefficientBaseConfig> extends AlgorithmFactory<LocalClusteringCoefficient, CONFIG> {

    @Override
    protected String taskName() {
        return "LocalClusteringCoefficient";
    }

    @Override
    protected LocalClusteringCoefficient build(
        Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
    ) {
        return new LocalClusteringCoefficient(
            graph,
            configuration,
            allocationTracker,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations
            .builder(LocalClusteringCoefficient.class)
            .perNode("local-clustering-coefficient", HugeDoubleArray::memoryEstimation);

        if(null == configuration.seedProperty()) {
            builder.add(
                "computed-triangle-counts",
                new IntersectingTriangleCountFactory<>().memoryEstimation(createTriangleCountConfig(configuration))
            );
        }

        return builder.build();
    }

    static TriangleCountStatsConfig createTriangleCountConfig(LocalClusteringCoefficientBaseConfig configuration) {
        return ImmutableTriangleCountStatsConfig.builder()
            .username(configuration.username())
            .graphName(configuration.graphName())
            .implicitCreateConfig(configuration.implicitCreateConfig())
            .concurrency(configuration.concurrency())
            .build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        var tasks = new ArrayList<Task>();
        if (config.seedProperty() == null) {
            tasks.add(IntersectingTriangleCountFactory.triangleCountProgressTask(graph));
        }
        tasks.add(Tasks.leaf("Calculate Local Clustering Coefficient", graph.nodeCount()));

        return Tasks.task(taskName(), tasks);
    }
}
