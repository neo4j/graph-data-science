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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class SpanningTreeAlgorithmFactory<CONFIG extends SpanningTreeBaseConfig> extends GraphAlgorithmFactory<Prim, CONFIG> {

    public Prim build(Graph graph, SpanningTreeParameters parameters, ProgressTracker progressTracker) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The Spanning Tree algorithm works only with undirected graphs. Please orient the edges properly");
        }
        return new Prim(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            progressTracker
        );
    }

    @Override
    public Prim build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        return build(graph, configuration.toParameters(), progressTracker);
    }

    @Override
    public String taskName() {
        return "SpanningTree";
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return new SpanningTreeMemoryEstimateDefinition().memoryEstimation(config);
    }

    public Task progressTask(Graph graph) {
        return Tasks.leaf(taskName(), graph.relationshipCount());
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph);
    }
}
