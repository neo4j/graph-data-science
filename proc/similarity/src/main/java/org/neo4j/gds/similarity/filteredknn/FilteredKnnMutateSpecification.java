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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityProc;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.alpha.knn.filtered.mutate", executionMode = MUTATE_RELATIONSHIP)
public class FilteredKnnMutateSpecification implements AlgorithmSpec<FilteredKnn, FilteredKnnResult, FilteredKnnMutateConfig, Stream<FilteredKnnMutateProcResult>, FilteredKnnFactory<FilteredKnnMutateConfig>> {
    @Override
    public String name() {
        return "FilteredKnnMutate";
    }

    @Override
    public FilteredKnnFactory<FilteredKnnMutateConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredKnnMutateConfig> newConfigFunction() {
        return (__, userInput) -> FilteredKnnMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FilteredKnn, FilteredKnnResult, FilteredKnnMutateConfig, Stream<FilteredKnnMutateProcResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Graph mutation failed",
            executionContext.log(),
            () -> {
                var config = computationResult.config();
                var resultBuilder = new FilteredKnnMutateProcResult.Builder();

                computationResult.result()
                    .ifPresent(result -> {
                        resultBuilder
                            .ranIterations(result.ranIterations())
                            .didConverge(result.didConverge())
                            .withNodePairsConsidered(result.nodePairsConsidered());


                        var filteredKnn = Objects.requireNonNull(computationResult.algorithm());

                        var mutateMillis = new AtomicLong();

                        SimilarityGraphResult similarityGraphResult;
                        try (ProgressTimer ignored = ProgressTimer.start(mutateMillis::addAndGet)) {
                            similarityGraphResult = FilteredKnnHelpers.computeToGraph(
                                computationResult.graph(),
                                filteredKnn.nodeCount(),
                                config.concurrency(),
                                result,
                                filteredKnn.executorService()
                            );

                            SimilarityProc.withGraphsizeAndTimings(
                                resultBuilder,
                                computationResult,
                                (ignore) -> similarityGraphResult
                            );

                            var similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();
                            if (SimilarityProc.shouldComputeHistogram(executionContext.returnColumns())) {
                                resultBuilder.withHistogram(SimilarityProc.computeHistogram(similarityGraph));
                            }


                            RelationshipType relationshipType = RelationshipType.of(config.mutateRelationshipType());
                            computationResult
                                .graphStore()
                                .addRelationshipType(
                                    SingleTypeRelationships.of(
                                        relationshipType,
                                        similarityGraph.relationshipTopology(),
                                        similarityGraphResult.direction(),
                                        similarityGraph.relationshipProperties(),
                                        Optional.of(RelationshipPropertySchema.of(
                                            config.mutateProperty(),
                                            ValueType.DOUBLE
                                        ))
                                    )
                                );
                        }

                        resultBuilder.withMutateMillis(mutateMillis.get());

                    });

                return Stream.of(resultBuilder.build());

            }
        );
    }
}
