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
package org.neo4j.gds.paths.astar;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.logging.Log;

public class AStarFactory<CONFIG extends ShortestPathAStarBaseConfig> implements AlgorithmFactory<AStar, CONFIG> {

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return AStar.memoryEstimation();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return DijkstraFactory.dijkstraProgressTask(graph);
    }

    @NotNull
    public static BatchingProgressLogger progressLogger(Graph graph, Log log) {
        return new BatchingProgressLogger(
            log,
            graph.relationshipCount(),
            "AStar",
            1
        );
    }

    @Override
    public AStar build(Graph graph, CONFIG configuration, AllocationTracker tracker, Log log, ProgressEventTracker eventTracker) {
        var progressLogger = progressLogger(graph, log);
        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger, eventTracker);
        return AStar.sourceTarget(graph, configuration, progressTracker, tracker);
    }
}
