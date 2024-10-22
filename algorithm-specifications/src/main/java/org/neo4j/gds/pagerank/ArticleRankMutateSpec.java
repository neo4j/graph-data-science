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

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankMutateResult;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.articleRank.mutate", description = Constants.ARTICLE_RANK_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ArticleRankMutateSpec implements AlgorithmSpec<PageRankAlgorithm<ArticleRankMutateConfig>, PageRankResult, ArticleRankMutateConfig, Stream<PageRankMutateResult>, ArticleRankAlgorithmFactory<ArticleRankMutateConfig>> {

    @Override
    public String name() {
        return "ArticleRank";
    }

    @Override
    public ArticleRankAlgorithmFactory<ArticleRankMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ArticleRankAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<ArticleRankMutateConfig> newConfigFunction() {
        return (__, userInput) -> ArticleRankMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<PageRankAlgorithm<ArticleRankMutateConfig>, PageRankResult, ArticleRankMutateConfig, Stream<PageRankMutateResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
