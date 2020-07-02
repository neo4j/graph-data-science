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
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.similarity.Computations;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithm;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.graphalgo.impl.similarity.SimilarityConfig;
import org.neo4j.graphalgo.results.SimilarityExporter;
import org.neo4j.graphalgo.results.SimilarityResult;
import org.neo4j.graphalgo.results.SimilarityStatsResult;
import org.neo4j.graphalgo.results.SimilaritySummaryResult;
import org.neo4j.graphalgo.similarity.nil.NullGraph;
import org.neo4j.graphalgo.similarity.nil.NullGraphLoader;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.ElementProjection.PROJECT_ALL;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

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

    Stream<SimilarityStatsResult> stats(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ComputationResult<ALGO, SimilarityAlgorithmResult, CONFIG> compute = compute(
            graphNameOrConfig,
            configuration
        );

        SimilarityAlgorithmResult result = compute.result();
        assert result != null;

        if (result.isEmpty()) {
            return Stream.of(SimilarityStatsResult.from(
                0,
                0,
                0,
                new AtomicLong(0),
                -1,
                new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT)
            ));
        }

        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
        result.stream().forEach(recorder -> {
            recorder.record(histogram);
            similarityPairs.getAndIncrement();
        });
        return Stream.of(SimilarityStatsResult.from(
            result.nodes(),
            result.sourceIdsLength(),
            result.targetIdsLength(),
            similarityPairs,
            result.computations().map(Computations::count).orElse(-1L),
            histogram
        ));
    }
    abstract ALGO newAlgo(CONFIG config);

    @Override
    protected final AlgorithmFactory<ALGO, CONFIG> algorithmFactory(CONFIG config) {
        return new AlphaAlgorithmFactory<ALGO, CONFIG>() {
            @Override
            public ALGO buildAlphaAlgo(
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
    protected Pair<CONFIG, Optional<String>> processInput(Object graphNameOrConfig, Map<String, Object> configuration) {
        if (graphNameOrConfig instanceof String) {
            throw new IllegalArgumentException("Similarity algorithms does not support named graphs");
        }
        if (graphNameOrConfig instanceof Map) {
            Map<String, Object> configMap = (Map<String, Object>) graphNameOrConfig;
            configMap.put(NODE_PROJECTION_KEY, PROJECT_ALL);
            configMap.put(RELATIONSHIP_PROJECTION_KEY, PROJECT_ALL);
        }
        return super.processInput(graphNameOrConfig, configuration);
    }

    @Override
    public GraphLoader newLoader(GraphCreateConfig createConfig, AllocationTracker tracker) {
        return new NullGraphLoader();
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
                new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT)
            )
        );
    }

    private Stream<SimilaritySummaryResult> writeAndAggregateResults(
        SimilarityAlgorithmResult algoResult,
        CONFIG config,
        TerminationFlag terminationFlag
    ) {
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        SimilarityExporter similarityExporter = new SimilarityExporter(
            SecureTransaction.of(api),
            config.writeRelationshipType(),
            config.writeProperty(),
            terminationFlag
        );
        similarityExporter.export(algoResult.stream().peek(recorder), config.writeBatchSize());

        return Stream.of(SimilaritySummaryResult.from(
            algoResult.nodes(),
            algoResult.sourceIdsLength(),
            algoResult.targetIdsLength(),
            similarityPairs,
            algoResult.computations().map(Computations::count).orElse(-1L),
            config.writeRelationshipType(),
            config.writeProperty(),
            histogram
        ));
    }
}
