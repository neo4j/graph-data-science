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
package org.neo4j.graphalgo.similarity.knn;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnMutateConfig;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.similarity.SimilarityMutateResult;
import org.neo4j.graphalgo.similarity.SimilarityProc;
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

import static org.neo4j.graphalgo.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.graphalgo.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.graphalgo.similarity.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.graphalgo.similarity.knn.KnnWriteProc.computeToGraph;
import static org.neo4j.procedure.Mode.READ;

public class KnnMutateProc extends MutatePropertyProc<Knn, Knn.Result, SimilarityMutateResult, KnnMutateConfig> {

    @Procedure(name = "gds.beta.knn.mutate", mode = READ)
    @Description(KNN_DESCRIPTION)
    public Stream<SimilarityMutateResult> mutate(
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
    protected AbstractResultBuilder<SimilarityMutateResult> resultBuilder(ComputationResult<Knn, Knn.Result, KnnMutateConfig> computeResult) {
        throw new UnsupportedOperationException("Knn handles result building individually.");
    }

    @Override
    protected AlgorithmFactory<Knn, KnnMutateConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    protected Stream<SimilarityMutateResult> mutate(ComputationResult<Knn, Knn.Result, KnnMutateConfig> computationResult) {
        return runWithExceptionLogging("Graph mutation failed", () -> {
            KnnMutateConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new SimilarityMutateResult(
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

            SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> resultBuilder =
                SimilarityProc.resultBuilder(
                    new SimilarityMutateResult.Builder(),
                    computationResult,
                    (ignore) -> similarityGraphResult
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
        SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> resultBuilder
    ) {
        HugeGraph similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();
        Relationships resultRelationships = similarityGraph.relationships();
        if (shouldComputeHistogram(callContext)) {
            resultBuilder.withHistogram(computeHistogram(similarityGraph));
        }
        return resultRelationships;
    }
}
