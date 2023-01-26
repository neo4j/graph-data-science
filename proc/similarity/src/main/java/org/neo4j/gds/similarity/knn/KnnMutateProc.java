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
package org.neo4j.gds.similarity.knn;

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
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityMutateResult;
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
import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.gds.similarity.knn.KnnWriteProc.computeToGraph;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.knn.mutate", description = KNN_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class KnnMutateProc extends AlgoBaseProc<Knn, Knn.Result, KnnMutateConfig, KnnMutateProc.Result> {

    @Procedure(name = "gds.knn.mutate", mode = READ)
    @Description(KNN_DESCRIPTION)
    public Stream<Result> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult =  compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Procedure(value = "gds.knn.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected KnnMutateConfig newConfig(String username, CypherMapWrapper config) {
        return KnnMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Knn, KnnMutateConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    public ComputationResultConsumer<Knn, Knn.Result, KnnMutateConfig, Stream<Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging("Graph mutation failed", () -> {
            KnnMutateConfig config = computationResult.config();
            Knn.Result result = computationResult.result();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new Result(
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

            Knn algorithm = Objects.requireNonNull(computationResult.algorithm());

            var mutateMillis = new AtomicLong();

            SimilarityGraphResult similarityGraphResult;
            try (ProgressTimer ignored = ProgressTimer.start(mutateMillis::addAndGet)) {
                similarityGraphResult = computeToGraph(
                    computationResult.graph(),
                    algorithm.nodeCount(),
                    config.concurrency(),
                    Objects.requireNonNull(result),
                    algorithm.executorService()
                );
            }

            Result.Builder resultBuilder = new Result.Builder()
                .ranIterations(result.ranIterations())
                .didConverge(result.didConverge())
                .withNodePairsConsidered(result.nodePairsConsidered());

            SimilarityProc.withGraphsizeAndTimings(resultBuilder, computationResult, (ignore) -> similarityGraphResult);


            try (ProgressTimer ignored = ProgressTimer.start(mutateMillis::addAndGet)) {
                var similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();
                computeHistogram(similarityGraph, resultBuilder);

                var relationshipType = RelationshipType.of(config.mutateRelationshipType());
                computationResult
                    .graphStore()
                    .addRelationshipType(
                        relationshipType,
                        SingleTypeRelationships.of(
                            relationshipType,
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
        Result.Builder resultBuilder
    ) {
        if (shouldComputeHistogram(callContext)) {
            resultBuilder.withHistogram(SimilarityProc.computeHistogram(similarityGraph));
        }
    }

    @SuppressWarnings("unused")
    public static class Result extends SimilarityMutateResult {
        public final long ranIterations;
        public final long nodePairsConsidered;
        public final boolean didConverge;

        public Result(
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationshipsWritten,
            Map<String, Object> similarityDistribution,
            boolean didConverge,
            long ranIterations,
            long nodePairsConsidered,
            Map<String, Object> configuration
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                postProcessingMillis,
                nodesCompared,
                relationshipsWritten,
                similarityDistribution,
                configuration
            );

            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.nodePairsConsidered = nodePairsConsidered;
        }

        public static class Builder extends SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> {
            private long ranIterations;
            private boolean didConverge;
            private long nodePairsConsidered;

            @Override
            public Result build() {
                return new Result(
                    preProcessingMillis,
                    computeMillis,
                    mutateMillis,
                    postProcessingMillis,
                    nodesCompared,
                    relationshipsWritten,
                    distribution(),
                    didConverge,
                    ranIterations,
                    nodePairsConsidered,
                    config.toMap()
                );
            }

            public Builder didConverge(boolean didConverge) {
                this.didConverge = didConverge;
                return this;
            }

            public Builder ranIterations(long ranIterations) {
                this.ranIterations = ranIterations;
                return this;
            }

            Builder withNodePairsConsidered(long nodePairsConsidered) {
                this.nodePairsConsidered = nodePairsConsidered;
                return this;
            }
        }
    }
}
