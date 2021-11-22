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
package org.neo4j.gds.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.ProcConfigParser;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.similarity.Computations;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithm;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.gds.impl.similarity.SimilarityConfig;
import org.neo4j.gds.results.SimilarityResult;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

abstract class AlphaSimilarityProc
    <ALGO extends SimilarityAlgorithm<ALGO, ?>, CONFIG extends SimilarityConfig>
    extends AlgoBaseProc<ALGO, SimilarityAlgorithmResult, CONFIG> {

    public static final String SIMILARITY_FAKE_GRAPH_NAME = "  SIM-NULL-GRAPH";

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

    Stream<AlphaSimilaritySummaryResult> write(
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

    Stream<AlphaSimilarityStatsResult> stats(
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
            return Stream.of(AlphaSimilarityStatsResult.from(
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
        return Stream.of(AlphaSimilarityStatsResult.from(
            result.nodes(),
            result.sourceIdsLength(),
            result.targetIdsLength(),
            similarityPairs,
            result.computations().map(Computations::count).orElse(-1L),
            histogram
        ));
    }
    abstract ALGO newAlgo(CONFIG config, AllocationTracker allocationTracker);

    abstract String taskName();

    @Override
    protected final AlgorithmFactory<ALGO, CONFIG> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            protected String taskName() {
                return AlphaSimilarityProc.this.taskName();
            }

            @Override
            protected ALGO build(
                Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
            ) {
                removeGraph(username(), databaseId());
                return newAlgo(configuration, allocationTracker);
            }
        };
    }

    @Override
    public ProcConfigParser<CONFIG> configParser() {
        return new AlphaSimilarityProcConfigParser<>(
            super.configParser(),
            databaseId()
        );
    }

    static void removeGraph(String username, NamedDatabaseId databaseId) {
        GraphStoreCatalog.remove(CatalogRequest.of(username, databaseId), SIMILARITY_FAKE_GRAPH_NAME, (gsc) -> {}, true);
    }

    private Stream<AlphaSimilaritySummaryResult> emptyStream(String writeRelationshipType, String writeProperty) {
        return Stream.of(
            AlphaSimilaritySummaryResult.from(
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

    private Stream<AlphaSimilaritySummaryResult> writeAndAggregateResults(
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
            TransactionContext.of(api, procedureTransaction),
            config.writeRelationshipType(),
            config.writeProperty(),
            terminationFlag
        );
        similarityExporter.export(algoResult.stream().peek(recorder), config.writeBatchSize());

        return Stream.of(AlphaSimilaritySummaryResult.from(
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
