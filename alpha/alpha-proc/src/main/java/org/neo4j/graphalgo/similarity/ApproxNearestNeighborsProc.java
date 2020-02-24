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
package org.neo4j.graphalgo.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.similarity.ApproxNearestNeighborsAlgorithm;
import org.neo4j.graphalgo.impl.similarity.ApproximateNearestNeighborsConfig;
import org.neo4j.graphalgo.impl.similarity.ApproximateNearestNeighborsConfigImpl;
import org.neo4j.graphalgo.impl.similarity.Computations;
import org.neo4j.graphalgo.impl.similarity.CosineAlgorithm;
import org.neo4j.graphalgo.impl.similarity.CosineConfig;
import org.neo4j.graphalgo.impl.similarity.EuclideanAlgorithm;
import org.neo4j.graphalgo.impl.similarity.EuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutableCosineConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutableEuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutableJaccardConfig;
import org.neo4j.graphalgo.impl.similarity.ImmutablePearsonConfig;
import org.neo4j.graphalgo.impl.similarity.JaccardAlgorithm;
import org.neo4j.graphalgo.impl.similarity.JaccardConfig;
import org.neo4j.graphalgo.impl.similarity.PearsonAlgorithm;
import org.neo4j.graphalgo.impl.similarity.PearsonConfig;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithm;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.ApproxSimilaritySummaryResult;
import org.neo4j.graphalgo.results.SimilarityExporter;
import org.neo4j.graphalgo.results.SimilarityResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ApproxNearestNeighborsProc extends SimilarityProc<ApproxNearestNeighborsAlgorithm<SimilarityInput>, ApproximateNearestNeighborsConfig> {

    public static final String ANN_DESCRIPTION = "The Approximate Nearest Neighbors algorithm constructs a k-Nearest Neighbors Graph for a set of objects based on a provided similarity algorithm";

    @Procedure(name = "gds.alpha.ml.ann.stream", mode = READ)
    @Description(ANN_DESCRIPTION)
    public Stream<SimilarityResult> annStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.ml.ann.write", mode = WRITE)
    @Description(ANN_DESCRIPTION)
    public Stream<ApproxSimilaritySummaryResult> annWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return writeResult(graphNameOrConfig, configuration);
    }

    @Override
    protected ApproximateNearestNeighborsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new ApproximateNearestNeighborsConfigImpl(
            graphName, maybeImplicitCreate, username, config
        );
    }

    @Override
    ApproxNearestNeighborsAlgorithm<SimilarityInput> newAlgo(ApproximateNearestNeighborsConfig config) {
        SimilarityAlgorithm<?, SimilarityInput> similarity =
            (SimilarityAlgorithm<?, SimilarityInput>) similarityAlgorithm(config);
        return new ApproxNearestNeighborsAlgorithm<>(
            config,
            similarity,
            api,
            log,
            Pools.DEFAULT,
            AllocationTracker.create()
        );
    }

    SimilarityAlgorithm<?, ? extends SimilarityInput> similarityAlgorithm(ApproximateNearestNeighborsConfig config) {
        switch (config.algorithm()) {
            case jaccard:
                JaccardConfig jaccardConfig = ImmutableJaccardConfig.builder().from(config).build();
                return new JaccardAlgorithm(jaccardConfig, api);
            case cosine:
                CosineConfig cosineConfig = ImmutableCosineConfig.builder().from(config).build();
                return new CosineAlgorithm(cosineConfig, api);
            case pearson:
                PearsonConfig pearsonConfig = ImmutablePearsonConfig.builder().from(config).build();
                return new PearsonAlgorithm(pearsonConfig, api);
            case euclidean:
                EuclideanConfig euclideanConfig = ImmutableEuclideanConfig.builder().from(config).build();
                return new EuclideanAlgorithm(euclideanConfig, api);
            default:
                throw new IllegalArgumentException("Unexpected value: " + config.algorithm() + " (sad java 😞)");
        }
    }

    Stream<ApproxSimilaritySummaryResult> writeResult(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ComputationResult<ApproxNearestNeighborsAlgorithm<SimilarityInput>, SimilarityAlgorithmResult, ApproximateNearestNeighborsConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        ApproximateNearestNeighborsConfig config = computationResult.config();
        SimilarityAlgorithmResult result = computationResult.result();
        assert result != null;

        if (result.isEmpty()) {
            return emptyStreamResult(result, config, computationResult.algorithm());
        }

        return writeAndAggregateANNResults(result, config, computationResult.algorithm());
    }

    private Stream<ApproxSimilaritySummaryResult> emptyStreamResult(
        SimilarityAlgorithmResult result,
        ApproximateNearestNeighborsConfig config,
        @Nullable ApproxNearestNeighborsAlgorithm<SimilarityInput> algorithm
    ) {
        return Stream.of(
            ApproxSimilaritySummaryResult.from(
                result.nodes(),
                0,
                result.computations().map(Computations::count).orElse(-1L),
                config.writeRelationshipType(),
                config.writeProperty(),
                config.write(),
                algorithm.iterations(),
                new DoubleHistogram(5)
            )
        );
    }

    private Stream<ApproxSimilaritySummaryResult> writeAndAggregateANNResults(
        SimilarityAlgorithmResult algoResult,
        ApproximateNearestNeighborsConfig config,
        @Nullable ApproxNearestNeighborsAlgorithm<SimilarityInput> algorithm
    ) {
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (config.write()) {
            SimilarityExporter similarityExporter = new SimilarityExporter(
                api,
                config.writeRelationshipType(),
                config.writeProperty(),
                algorithm.getTerminationFlag()
            );
            similarityExporter.export(algoResult.stream().peek(recorder), config.writeBatchSize());
        } else {
            algoResult.stream().forEach(recorder);
        }

        return Stream.of(
            ApproxSimilaritySummaryResult.from(
                algoResult.nodes(),
                similarityPairs.get(),
                algoResult.computations().map(Computations::count).orElse(-1L),
                config.writeRelationshipType(),
                config.writeProperty(),
                config.write(),
                algorithm.iterations(),
                histogram
            )
        );
    }
}
