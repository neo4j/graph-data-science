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
package org.neo4j.gds.similarity.filteredknn;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityWriteConsumer;

import java.util.Objects;
import java.util.stream.Stream;

public class FilteredKnnWriteSpecification implements AlgorithmSpec<FilteredKnn, FilteredKnnResult, FilteredKnnWriteConfig, Stream<FilteredKnnWriteProcResult>, FilteredKnnFactory<FilteredKnnWriteConfig>> {
    @Override
    public String name() {
        return "FilteredKnnWrite";
    }

    @Override
    public FilteredKnnFactory<FilteredKnnWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new FilteredKnnFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredKnnWriteConfig> newConfigFunction() {
        return (__, userInput) -> FilteredKnnWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FilteredKnn, FilteredKnnResult, FilteredKnnWriteConfig, Stream<FilteredKnnWriteProcResult>> computationResultConsumer() {
        return new SimilarityWriteConsumer<>(
            this::resultBuilderFunction,
            this::similarityGraphResult,
            name()
        );
    }

    private FilteredKnnWriteProcResult.Builder resultBuilderFunction(ComputationResult<FilteredKnn, FilteredKnnResult, FilteredKnnWriteConfig> computationResult) {
        var builder = new FilteredKnnWriteProcResult.Builder();

        computationResult.result().ifPresent(result -> {
            builder
                .withDidConverge(result.didConverge())
                .withNodePairsConsidered(result.nodePairsConsidered())
                .withRanIterations(result.ranIterations());
        });

        return builder;
    }

    protected SimilarityGraphResult similarityGraphResult(ComputationResult<FilteredKnn, FilteredKnnResult, FilteredKnnWriteConfig> computationResult) {
        if (computationResult.result().isEmpty()) {
            return new SimilarityGraphResult(computationResult.graph(), 0, false);
        }

        FilteredKnn algorithm = Objects.requireNonNull(computationResult.algorithm());
        FilteredKnnWriteConfig config = computationResult.config();
        return FilteredKnnHelpers.computeToGraph(
            computationResult.graph(),
            algorithm.nodeCount(),
            config.concurrency(),
            computationResult.result().get(),
            algorithm.executorService()
        );
    }
}
