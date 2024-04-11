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
package org.neo4j.gds.dag.topologicalsort;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public class TopologicalSortFactory<CONFIG extends TopologicalSortBaseConfig> extends GraphAlgorithmFactory<TopologicalSort, CONFIG> {
    @Override
    public TopologicalSort build(Graph graph, TopologicalSortBaseConfig configuration, ProgressTracker progressTracker) {
        return new TopologicalSort(
            graph,
            progressTracker,
            configuration.typedConcurrency(),
            configuration.computeMaxDistanceFromSource()
        );
    }

    @Override
    public String taskName() {
        return "TopologicalSort";
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        var initializationTask = Tasks.leaf("Initialization", graph.nodeCount());
        var traversalTask = Tasks.leaf("Traversal", graph.nodeCount());

        return Tasks.task("TopologicalSort", List.of(initializationTask, traversalTask));
    }
}
