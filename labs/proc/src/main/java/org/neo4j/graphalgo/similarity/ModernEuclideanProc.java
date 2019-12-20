/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.results.SimilarityExporter;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.results.SimilaritySummaryResult;
import org.neo4j.graphalgo.impl.similarity.Computations;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanAlgorithm.EuclideanSimilarityResult;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanConfigImpl;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ModernEuclideanProc extends AlgoBaseProc<ModernEuclideanAlgorithm, EuclideanSimilarityResult, ModernEuclideanConfig> {

    @Procedure(name = "gds.alpha.similarity.euclidean.stream", mode = READ)
    public Stream<SimilarityResult> euclideanStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ModernEuclideanAlgorithm, EuclideanSimilarityResult, ModernEuclideanConfig> compute = compute(
            graphNameOrConfig,
            configuration
        );
        return compute.result().stream();
    }

    @Procedure(name = "gds.alpha.similarity.euclidean.write", mode = Mode.WRITE)
    public Stream<SimilaritySummaryResult> euclidean(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ModernEuclideanAlgorithm, EuclideanSimilarityResult, ModernEuclideanConfig> compute = compute(
            graphNameOrConfig,
            configuration
        );

        ModernEuclideanConfig config = compute.config();
        EuclideanSimilarityResult result = compute.result();

        if(result.isEmpty()) {
            return emptyStream(config.writeRelationshipType(), config.writeProperty());
        }

        return writeAndAggregateResults(result, config, compute.algorithm().getTerminationFlag());
    }

    @Override
    protected ModernEuclideanConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new ModernEuclideanConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    protected AlgorithmFactory<ModernEuclideanAlgorithm, ModernEuclideanConfig> algorithmFactory(ModernEuclideanConfig config) {

        return new AlgorithmFactory<ModernEuclideanAlgorithm, ModernEuclideanConfig>() {

            @Override
            public ModernEuclideanAlgorithm build(
                Graph graph,
                ModernEuclideanConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ModernEuclideanAlgorithm(config, api);
            }

            @Override
            public MemoryEstimation memoryEstimation(ModernEuclideanConfig configuration) {
                throw new IllegalArgumentException("Memory estimation not implemented for Euclidean Similarity");
            }
        };
    }

    @Override
    protected Graph createGraph(Pair<ModernEuclideanConfig, Optional<String>> configAndName) {
        if (configAndName.other().isPresent()) {
            throw new IllegalArgumentException("Euclidean Similarity does not run on an explicitly created graph");
        }
        return new NullGraph();
    }

    protected Stream<SimilaritySummaryResult> emptyStream(String writeRelationshipType, String writeProperty) {
        return Stream.of(
            SimilaritySummaryResult.from(
                0,
                0,
                0,
                new AtomicLong(0),
                -1,
                writeRelationshipType,
                writeProperty,
                false,
                new DoubleHistogram(5)
            )
        );
    }

    Stream<SimilaritySummaryResult> writeAndAggregateResults(
        ModernEuclideanAlgorithm.EuclideanSimilarityResult algoResult,
        ModernEuclideanConfig config,
        TerminationFlag terminationFlag
    ) {
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (config.write()) {
            SimilarityExporter similarityExporter = new SimilarityExporter(api, config.writeRelationshipType(), config.writeProperty(), terminationFlag);
            similarityExporter.export(algoResult.stream().peek(recorder), config.writeBatchSize());
        } else {
            algoResult.stream().forEach(recorder);
        }

        return Stream.of(SimilaritySummaryResult.from(
            algoResult.nodes(),
            algoResult.sourceIdsLength(),
            algoResult.targetIdsLength(),
            similarityPairs,
            algoResult.computations().map(Computations::count).orElse(-1L),
            config.writeRelationshipType(),
            config.writeProperty(),
            config.write(),
            histogram
        ));
    }
}
