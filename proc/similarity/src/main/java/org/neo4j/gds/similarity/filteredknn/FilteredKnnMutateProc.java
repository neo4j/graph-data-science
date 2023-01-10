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

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.knn.filtered.mutate", executionMode = MUTATE_RELATIONSHIP)
public class FilteredKnnMutateProc extends AlgoBaseProc<FilteredKnn, FilteredKnnResult, FilteredKnnMutateConfig, FilteredKnnMutateProcResult> {
    @Procedure(name = "gds.alpha.knn.filtered.mutate", mode = READ)
    @Description(FilteredKnnConstants.PROCEDURE_DESCRIPTION)
    public Stream<FilteredKnnMutateProcResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    protected FilteredKnnMutateConfig newConfig(String username, CypherMapWrapper config) {
        return FilteredKnnMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<FilteredKnn, FilteredKnnMutateConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    public ComputationResultConsumer<FilteredKnn, FilteredKnnResult, FilteredKnnMutateConfig, Stream<FilteredKnnMutateProcResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging("Graph mutation failed", () -> {
            FilteredKnnMutateConfig config = computationResult.config();
            FilteredKnnResult result = computationResult.result();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new FilteredKnnMutateProcResult(
                        computationResult.preProcessingMillis(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyMap(),
                        true,
                        0,
                        0,
                        config.toMap()
                    )
                );
            }

            FilteredKnn algorithm = Objects.requireNonNull(computationResult.algorithm());

            var mutateMillis = new AtomicLong();

            SimilarityGraphResult similarityGraphResult;
            try (ProgressTimer ignored = ProgressTimer.start(mutateMillis::addAndGet)) {
                similarityGraphResult = FilteredKnnHelpers.computeToGraph(
                    computationResult.graph(),
                    algorithm.nodeCount(),
                    config.concurrency(),
                    Objects.requireNonNull(result),
                    algorithm.executorService()
                );
            }

            FilteredKnnMutateProcResult.Builder resultBuilder = new FilteredKnnMutateProcResult.Builder()
                .ranIterations(result.ranIterations())
                .didConverge(result.didConverge())
                .withNodePairsConsidered(result.nodePairsConsidered());

            SimilarityProc.withGraphsizeAndTimings(resultBuilder, computationResult, (ignore) -> similarityGraphResult);


            try (ProgressTimer ignored = ProgressTimer.start(mutateMillis::addAndGet)) {
                var similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();
                computeHistogram(similarityGraph, resultBuilder);

                computationResult
                    .graphStore()
                    .addRelationshipType(
                        RelationshipType.of(config.mutateRelationshipType()),
                        SingleTypeRelationships.of(
                            similarityGraph.relationshipTopology(),
                            similarityGraphResult.direction(),
                            similarityGraph.relationshipProperties(),
                            Optional.of(RelationshipPropertySchema.of(config.mutateProperty(), ValueType.DOUBLE))
                        )
                    );
            }

            resultBuilder.withMutateMillis(mutateMillis.get());

            return Stream.of(resultBuilder.build());
        });
    }

    private void computeHistogram(
        Graph similarityGraph,
        FilteredKnnMutateProcResult.Builder resultBuilder
    ) {
        if (shouldComputeHistogram(callContext)) {
            resultBuilder.withHistogram(SimilarityProc.computeHistogram(similarityGraph));
        }
    }
}
