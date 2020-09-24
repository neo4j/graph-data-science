/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.knn;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.nodesim.SimilarityGraphResult;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.NumberType;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.graphalgo.knn.KnnWriteProc.computeToGraph;
import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.computeHistogram;
import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

public class KnnMutateProc extends MutateProc<Knn, Knn.Result, KnnMutateProc.MutateResult, KnnMutateConfig> {

    @Procedure(name = "gds.beta.knn.mutate", mode = READ)
    @Description(KNN_DESCRIPTION)
    public Stream<KnnMutateProc.MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.beta.knn.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateMutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected KnnMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return KnnMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<Knn, Knn.Result, KnnMutateConfig> computeResult) {
        throw new UnsupportedOperationException("Knn handles result building individually.");
    }

    @Override
    protected AlgorithmFactory<Knn, KnnMutateConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    protected Stream<MutateResult> mutate(ComputationResult<Knn, Knn.Result, KnnMutateConfig> computationResult) {
        return runWithExceptionLogging("Graph mutation failed", () -> {
            KnnMutateConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new MutateResult(
                        computationResult.createMillis(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyMap(),
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
                    Objects.requireNonNull(computationResult.result()),
                    algorithm.context()
                );
            }

            KnnProc.KnnResultBuilder<KnnMutateProc.MutateResult> resultBuilder =
                KnnProc.resultBuilder(
                    new KnnMutateProc.MutateResult.Builder(),
                    computationResult,
                    similarityGraphResult
                );

            try (ProgressTimer ignored = ProgressTimer.start(mutateMillis::addAndGet)) {
                Relationships resultRelationships = getRelationships(
                    similarityGraphResult,
                    resultBuilder
                );

                computationResult
                    .graphStore()
                    .addRelationshipType(
                        RelationshipType.of(config.mutateRelationshipType()),
                        Optional.of(config.mutateProperty()),
                        Optional.of(NumberType.FLOATING_POINT),
                        resultRelationships
                    );
            }

            resultBuilder.withMutateMillis(mutateMillis.get());

            return Stream.of(resultBuilder.build());
        });
    }

    private Relationships getRelationships(
        SimilarityGraphResult similarityGraphResult,
        KnnProc.KnnResultBuilder<MutateResult> resultBuilder
    ) {
        HugeGraph similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();
        Relationships resultRelationships = similarityGraph.relationships();
        if (shouldComputeHistogram(callContext)) {
            resultBuilder.withHistogram(computeHistogram(similarityGraph));
        }
        return resultRelationships;
    }

    public static class MutateResult {
        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long postProcessingMillis;

        public final long nodesCompared;
        public final long relationshipsWritten;

        public final Map<String, Object> similarityDistribution;
        public final Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationshipsWritten,
            Map<String, Object> similarityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.relationshipsWritten = relationshipsWritten;
            this.similarityDistribution = similarityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends KnnProc.KnnResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    postProcessingMillis,
                    nodesCompared,
                    relationshipsWritten,
                    distribution(),
                    config.toMap()
                );
            }
        }
    }
}
