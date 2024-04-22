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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.wcc.WccAlgorithmFactory;

public class NodeSimilarityFactory<CONFIG extends NodeSimilarityBaseConfig> extends GraphAlgorithmFactory<NodeSimilarity, CONFIG> {

    @Override
    public String taskName() {
        return "NodeSimilarity";
    }

    public NodeSimilarity build(
        Graph graph,
        NodeSimilarityParameters parameters,
        Concurrency concurrency,
        ProgressTracker progressTracker
    ) {
        return new NodeSimilarity(
            graph,
            parameters,
            concurrency,
            DefaultPool.INSTANCE,
            progressTracker,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING
        );
    }

    @Override
    public NodeSimilarity build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        return build(graph, configuration.toParameters(), configuration.typedConcurrency(), progressTracker);
    }



    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return new NodeSimilarityMemoryEstimateDefinition(config.toMemoryEstimateParameters()).memoryEstimation();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph, config.useComponents().computeComponents());
    }

    public Task progressTask(Graph graph, boolean runWCC) {
        return Tasks.task(
            taskName(),
            runWCC
                ? Tasks.task("prepare", new WccAlgorithmFactory<>().progressTask(graph), Tasks.leaf("initialize", graph.relationshipCount()))
                : Tasks.leaf("prepare", graph.relationshipCount()),
            Tasks.leaf("compare node pairs")
        );
    }
}
