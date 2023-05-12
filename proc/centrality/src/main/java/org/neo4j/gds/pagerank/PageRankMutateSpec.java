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

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.pagerank.PageRankProcCompanion.PAGE_RANK_DESCRIPTION;

@GdsCallable(name = "gds.pageRank.mutate", description = PAGE_RANK_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class PageRankMutateSpec implements AlgorithmSpec<PageRankAlgorithm, PageRankResult,PageRankMutateConfig,Stream<MutateResult>,PageRankAlgorithmFactory<PageRankMutateConfig>> {

    @Override
    public String name() {
        return "PageRankMutate";
    }

    @Override
    public PageRankAlgorithmFactory<PageRankMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new PageRankAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<PageRankMutateConfig> newConfigFunction() {
        return (___,config) -> PageRankMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<PageRankAlgorithm, PageRankResult, PageRankMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult ->
                List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                    computationResult.result().get().scores().asNodeProperties())
            ),this::resultBuilder);
    }

    private AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<PageRankAlgorithm, PageRankResult, PageRankMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new MutateResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );

        computationResult.result().ifPresent(result -> {
            builder
                .withDidConverge(result.didConverge())
                .withRanIterations(result.iterations())
                .withCentralityFunction(result.scores()::get)
                .withScalerVariant(computationResult.config().scaler());
        });

        return builder;
    }


}
