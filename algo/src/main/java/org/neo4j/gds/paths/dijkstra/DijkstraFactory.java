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
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.paths.AllShortestPathsBaseConfig;
import org.neo4j.gds.paths.ShortestPathBaseConfig;
import org.neo4j.gds.paths.SourceTargetShortestPathBaseConfig;

import java.util.Optional;

public abstract class DijkstraFactory<CONFIG extends ShortestPathBaseConfig> extends GraphAlgorithmFactory<Dijkstra, CONFIG> {

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration);
    }

    @Override
    public String taskName() {
        return "Dijkstra";
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return dijkstraProgressTask(taskName(), graph);
    }


    @NotNull
    public static Task dijkstraProgressTask(String taskName, Graph graph) {
        return Tasks.leaf(taskName, graph.relationshipCount());
    }

    public static class SourceTargetDijkstraFactory<T extends SourceTargetShortestPathBaseConfig> extends
        DijkstraFactory<T> {
        @Override
        public Dijkstra build(
            Graph graph,
            T configuration,
            ProgressTracker progressTracker
        ) {
            return Dijkstra.sourceTarget(
                graph,
                configuration,
                Optional.empty(),
                progressTracker
            );
        }
    }

    public static class AllShortestPathsDijkstraFactory<T extends AllShortestPathsBaseConfig> extends DijkstraFactory<T> {
        @Override
        public Dijkstra build(
            Graph graph,
            T configuration,
            ProgressTracker progressTracker
        ) {
            return Dijkstra.singleSource(
                graph,
                configuration,
                Optional.empty(),
                progressTracker
            );
        }
    }
}
