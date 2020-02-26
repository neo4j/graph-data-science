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
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.similarity.Computations;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithm;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.graphalgo.impl.similarity.SimilarityConfig;
import org.neo4j.graphalgo.results.SimilarityExporter;
import org.neo4j.graphalgo.results.SimilarityResult;
import org.neo4j.graphalgo.results.SimilaritySummaryResult;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

abstract class SimilarityProc
    <ALGO extends SimilarityAlgorithm<ALGO, ?>, CONFIG extends SimilarityConfig>
    extends AlgoBaseProc<ALGO, SimilarityAlgorithmResult, CONFIG> {

    Stream<SimilarityResult> stream(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ComputationResult<ALGO, SimilarityAlgorithmResult, CONFIG> compute = compute(
            graphNameOrConfig,
            configuration
        );

        SimilarityAlgorithmResult result = compute.result();
        assert result != null;

        return result.stream();
    }

    Stream<SimilaritySummaryResult> write(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ComputationResult<ALGO, SimilarityAlgorithmResult, CONFIG> compute = compute(
            graphNameOrConfig,
            configuration
        );

        CONFIG config = compute.config();
        SimilarityAlgorithmResult result = compute.result();
        assert result != null;

        if (result.isEmpty()) {
            return emptyStream(config.writeRelationshipType(), config.writeProperty());
        }

        return writeAndAggregateResults(result, config, compute.algorithm().getTerminationFlag());
    }

    abstract ALGO newAlgo(CONFIG config);

    @Override
    protected final AlgorithmFactory<ALGO, CONFIG> algorithmFactory(CONFIG config) {
        return new AlphaAlgorithmFactory<ALGO, CONFIG>() {
            @Override
            public ALGO build(
                Graph graph,
                CONFIG configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return newAlgo(config);
            }
        };
    }

    @Override
    protected final Graph createGraph(Pair<CONFIG, Optional<String>> configAndName) {
        if (configAndName.getTwo().isPresent()) {
            throw new IllegalArgumentException("Similarity does not run on an explicitly created graph");
        }
        return new NullGraph();
    }

    private Stream<SimilaritySummaryResult> emptyStream(String writeRelationshipType, String writeProperty) {
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

    private Stream<SimilaritySummaryResult> writeAndAggregateResults(
        SimilarityAlgorithmResult algoResult,
        CONFIG config,
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
}
