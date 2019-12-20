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
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.results.SimilarityExporter;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.results.SimilaritySummaryResult;
import org.neo4j.graphalgo.impl.similarity.Computations;
import org.neo4j.graphalgo.impl.similarity.modern.ModernCosineAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernCosineConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernCosineConfigImpl;
import org.neo4j.graphalgo.impl.similarity.modern.ModernSimilarityAlgorithmResult;
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

public class ModernCosineProc extends AlgoBaseProc<ModernCosineAlgorithm, ModernSimilarityAlgorithmResult, ModernCosineConfig> {

    @Procedure(name = "gds.alpha.similarity.cosine.stream", mode = READ)
    public Stream<SimilarityResult> cosineStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ModernCosineAlgorithm, ModernSimilarityAlgorithmResult, ModernCosineConfig> compute = compute(
            graphNameOrConfig,
            configuration
        );
        return compute.result().stream();
    }

    @Procedure(name = "gds.alpha.similarity.cosine.write", mode = Mode.WRITE)
    public Stream<SimilaritySummaryResult> cosine(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ModernCosineAlgorithm, ModernSimilarityAlgorithmResult, ModernCosineConfig> compute = compute(
            graphNameOrConfig,
            configuration
        );

        ModernCosineConfig config = compute.config();
        ModernSimilarityAlgorithmResult result = compute.result();

        if (result.isEmpty()) {
            return emptyStream(config.writeRelationshipType(), config.writeProperty());
        }

        return writeAndAggregateResults(result, config, compute.algorithm().getTerminationFlag());
    }

    @Override
    protected Graph createGraph(Pair<ModernCosineConfig, Optional<String>> configAndName) {
        if (configAndName.other().isPresent()) {
            throw new IllegalArgumentException("Cosine Similarity does not run on an explicitly created graph");
        }
        return new NullGraph();
    }

    @Override
    protected ModernCosineConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new ModernCosineConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    protected AlgorithmFactory<ModernCosineAlgorithm, ModernCosineConfig> algorithmFactory(ModernCosineConfig config) {
        return new AlphaAlgorithmFactory<ModernCosineAlgorithm, ModernCosineConfig>() {
            @Override
            public ModernCosineAlgorithm build(
                Graph graph,
                ModernCosineConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ModernCosineAlgorithm(config, api);
            }
        };
    }

    Stream<SimilaritySummaryResult> writeAndAggregateResults(
        ModernSimilarityAlgorithmResult algoResult,
        ModernCosineConfig config,
        TerminationFlag terminationFlag
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
                terminationFlag
            );
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
}
