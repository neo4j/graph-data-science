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
package org.neo4j.gds.similarity.filterednodesim;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityParameters;
import org.neo4j.gds.wcc.WccAlgorithmFactory;

public class FilteredNodeSimilarityFactory<CONFIG extends FilteredNodeSimilarityBaseConfig> extends GraphAlgorithmFactory<NodeSimilarity, CONFIG> {

    @Override
    public String taskName() {
        return "FilteredNodeSimilarity";
    }

    public NodeSimilarity build(
        Graph graph,
        NodeSimilarityParameters parameters,
        int concurrency,
        NodeFilter sourceNodeFilter,
        NodeFilter targetNodeFilter,
        ProgressTracker progressTracker
    ) {
        return new NodeSimilarity(
            graph,
            parameters,
            concurrency,
            DefaultPool.INSTANCE,
            progressTracker,
            sourceNodeFilter,
            targetNodeFilter
        );
    }

    @Override
    public NodeSimilarity build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        var sourceNodeFilter = configuration.sourceNodeFilter().toNodeFilter(graph);
        var targetNodeFilter = configuration.targetNodeFilter().toNodeFilter(graph);
        return build(
            graph,
            configuration.toParameters(),
            configuration.concurrency(),
            sourceNodeFilter,
            targetNodeFilter,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return new FilteredNodeSimilarityMemoryEstimateDefinition(config.toParameters()).memoryEstimation();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            taskName(),
            progressTask(graph, config.useComponents().computeComponents()),
            Tasks.leaf("compare node pairs")
        );
    }

    private Task progressTask(Graph graph, boolean runWcc) {
        if (runWcc) {
            return Tasks.task(
                "prepare",
                new WccAlgorithmFactory<>().progressTask(graph),
                Tasks.leaf("initialize", graph.relationshipCount())
            );
        }
        return Tasks.leaf("prepare", graph.relationshipCount());
    }
}
