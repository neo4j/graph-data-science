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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.logging.Log;

public class LocalClusteringCoefficientFactory<CONFIG extends LocalClusteringCoefficientBaseConfig> implements AlgorithmFactory<LocalClusteringCoefficient, CONFIG> {

    @Override
    public LocalClusteringCoefficient build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, Log log,
        ProgressEventTracker eventTracker
    ) {

        ProgressLogger progressLogger = new BatchingProgressLogger(
            log,
            graph.nodeCount(),
            getClass().getSimpleName(),
            configuration.concurrency(),
            eventTracker
        );

        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger);

        return new LocalClusteringCoefficient(
            graph,
            configuration,
            tracker,
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
        return config.seedProperty() == null
            ? Tasks.task("LocalClusterCoefficient", IntersectingTriangleCountFactory.triangleCountProgressTask(graph))
            : Tasks.leaf("LocalClusterCoefficient");
    }
}
