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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityProc;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;

@GdsCallable(name = "gds.alpha.knn.filtered.stats", executionMode = STATS)
public class FilteredKnnStatsSpecification implements AlgorithmSpec<FilteredKnn, FilteredKnnResult, FilteredKnnStatsConfig, Stream<FilteredKnnStatsResult>, FilteredKnnFactory<FilteredKnnStatsConfig>> {
    @Override
    public String name() {
        return "FilteredKnnStats";
    }

    @Override
    public FilteredKnnFactory<FilteredKnnStatsConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredKnnStatsConfig> newConfigFunction() {
        return (__, userInput) -> FilteredKnnStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FilteredKnn, FilteredKnnResult, FilteredKnnStatsConfig, Stream<FilteredKnnStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Graph stats failed",
            executionContext.log(),
            () -> {
                var config = computationResult.config();
                var resultBuilder = new FilteredKnnStatsResult.Builder();

                Optional.ofNullable(computationResult.result())
                    .ifPresent(result -> {
                        resultBuilder
                            .withRanIterations(result.ranIterations())
                            .withNodePairsConsidered(result.nodePairsConsidered())
                            .withDidConverge(result.didConverge());

                        SimilarityProc.resultBuilderWithTimings(resultBuilder, computationResult);
                        var filteredKnn = Objects.requireNonNull(computationResult.algorithm());

                        if (shouldComputeHistogram(executionContext.returnColumns())) {
                            try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                                SimilarityGraphResult similarityGraphResult = FilteredKnnHelpers.computeToGraph(
                                    computationResult.graph(),
                                    filteredKnn.nodeCount(),
                                    config.concurrency(),
                                    result,
                                    filteredKnn.executorService()
                                );

                                Graph similarityGraph = similarityGraphResult.similarityGraph();

                                resultBuilder
                                    .withHistogram(computeHistogram(similarityGraph))
                                    .withNodesCompared(similarityGraphResult.comparedNodes())
                                    .withRelationshipsWritten(similarityGraph.relationshipCount());
                            }
                        } else {
                            resultBuilder
                                .withNodesCompared(filteredKnn.nodeCount())
                                .withRelationshipsWritten(result.numberOfSimilarityPairs());
                        }
                    });


                return Stream.of(resultBuilder.build());
            });
    }
}
