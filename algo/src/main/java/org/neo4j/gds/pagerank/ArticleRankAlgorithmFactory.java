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
package org.neo4j.gds.pagerank;

import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.termination.TerminationFlag;

import static org.neo4j.gds.pagerank.PageRankVariant.ARTICLE_RANK;

public class ArticleRankAlgorithmFactory<C extends ArticleRankConfig> extends GraphAlgorithmFactory<PageRankAlgorithm<C>, C> {

    @Override
    public String taskName() {
        return ARTICLE_RANK.taskName();
    }

    @Override
    public PageRankAlgorithm<C> build(
        Graph graph,
        C configuration,
        ProgressTracker progressTracker
    ) {

        var degreeFunction = DegreeFunctions.pageRankDegreeFunction(
            graph,
            configuration.hasRelationshipWeightProperty(), configuration.concurrency()
        );

        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());
        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        double avgDegree = DegreeFunctions.averageDegree(graph, configuration.concurrency());
        var computation = new ArticleRankComputation<>(configuration, mappedSourceNodes, degreeFunction, avgDegree);

        return new PageRankAlgorithm<>(
            graph,
            configuration,
            computation,
            ARTICLE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Override
    public Task progressTask(Graph graph, C config) {
        return Pregel.progressTask(graph, config, taskName());
    }

    @Override
    public MemoryEstimation memoryEstimation(C configuration) {
        return new PageRankMemoryEstimateDefinition().memoryEstimation();
    }
}
