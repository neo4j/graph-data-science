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
package org.neo4j.gds.paths.dijkstra;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.gds.paths.AllShortestPathsBaseConfig;
import org.neo4j.gds.paths.ShortestPathBaseConfig;
import org.neo4j.logging.Log;

import java.util.Optional;

public abstract class DijkstraFactory<T extends AlgoBaseConfig & RelationshipWeightConfig> implements AlgorithmFactory<Dijkstra, T> {

    @Override
    public MemoryEstimation memoryEstimation(T configuration) {
        return Dijkstra.memoryEstimation(false);
    }

    @Override
    public Task progressTask(Graph graph, T config) {
        return dijkstraProgressTask(graph);
    }

    @NotNull
    public static Task dijkstraProgressTask(Graph graph) {
        return Tasks.leaf("compute", graph.relationshipCount());
    }

    public static ProgressTracker progressTracker(
        Task task,
        Graph graph,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        return new TaskProgressTracker(task, progressLogger(graph, log, eventTracker), eventTracker);
    }

    @NotNull
    private static BatchingProgressLogger progressLogger(
        Graph graph,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        return new BatchingProgressLogger(
            log,
            graph.relationshipCount(),
            "Dijkstra",
            1,
            eventTracker
        );
    }

    public static <T extends ShortestPathBaseConfig> DijkstraFactory<T> sourceTarget() {
        return new DijkstraFactory<>() {
            @Override
            public Dijkstra build(
                Graph graph,
                T configuration,
                AllocationTracker tracker,
                Log log,
                ProgressEventTracker eventTracker
            ) {
                var progressTask = progressTask(graph, configuration);
                return Dijkstra.sourceTarget(
                    graph,
                    configuration,
                    Optional.empty(),
                    progressTracker(progressTask, graph, log, eventTracker),
                    tracker
                );
            }
        };
    }

    public static <T extends AllShortestPathsBaseConfig> DijkstraFactory<T> singleSource() {
        return new DijkstraFactory<>() {
            @Override
            public Dijkstra build(
                Graph graph,
                T configuration,
                AllocationTracker tracker,
                Log log,
                ProgressEventTracker eventTracker
            ) {
                var progressTask = progressTask(graph, configuration);
                return Dijkstra.singleSource(
                    graph,
                    configuration,
                    Optional.empty(),
                    progressTracker(progressTask, graph, log, eventTracker),
                    tracker
                );
            }
        };
    }
}
