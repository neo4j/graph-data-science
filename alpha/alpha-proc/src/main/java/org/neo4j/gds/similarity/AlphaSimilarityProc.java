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
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcConfigParser;
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

public abstract class AlphaSimilarityProc
    <ALGO extends SimilarityAlgorithm<ALGO, ?>, CONFIG extends SimilarityConfig, PROC_RESULT>
    extends AlgoBaseProc<ALGO, SimilarityAlgorithmResult, CONFIG, PROC_RESULT> {

    public static final String SIMILARITY_FAKE_GRAPH_NAME = "  SIM-NULL-GRAPH";

    protected Stream<SimilarityResult> stream(
        Map<String, Object> configuration
    ) {
        ComputationResult<ALGO, SimilarityAlgorithmResult, CONFIG> compute = compute(
            AlphaSimilarityProc.SIMILARITY_FAKE_GRAPH_NAME,
            configuration
        );

        return streamResultConsumer().consume(compute, executionContext());
    }

    protected ComputationResultConsumer<ALGO, SimilarityAlgorithmResult, CONFIG, Stream<SimilarityResult>> streamResultConsumer() {
        return (computationResult, executionContext) -> {
            SimilarityAlgorithmResult result = computationResult.result();
            assert result != null;

            return result.stream();
        };
    }

    protected Stream<AlphaSimilaritySummaryResult> write(
        Map<String, Object> configuration
    ) {
        ComputationResult<ALGO, SimilarityAlgorithmResult, CONFIG> computationResult = compute(
            AlphaSimilarityProc.SIMILARITY_FAKE_GRAPH_NAME,
            configuration
        );
        return writeResultConsumer().consume(computationResult, executionContext());
    }

    protected ComputationResultConsumer<ALGO, SimilarityAlgorithmResult, CONFIG, Stream<AlphaSimilaritySummaryResult>> writeResultConsumer() {
        return (computationResult, executionContext) -> {
            CONFIG config = computationResult.config();
            SimilarityAlgorithmResult result = computationResult.result();
            assert result != null;

            if (result.isEmpty()) {
                return emptyStream(config.writeRelationshipType(), config.writeProperty());
            }

            return writeAndAggregateResults(result, config, computationResult.algorithm().getTerminationFlag());
        };
    }

    protected Stream<AlphaSimilarityStatsResult> stats(
        Map<String, Object> configuration
    ) {
        ComputationResult<ALGO, SimilarityAlgorithmResult, CONFIG> computationResult = compute(
            AlphaSimilarityProc.SIMILARITY_FAKE_GRAPH_NAME,
            configuration
        );
       return statsResultConsumer().consume(computationResult, executionContext());
    }

    protected ComputationResultConsumer<ALGO, SimilarityAlgorithmResult, CONFIG, Stream<AlphaSimilarityStatsResult>> statsResultConsumer() {
        return (computationResult, executionContext) -> {
            SimilarityAlgorithmResult result = computationResult.result();
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
        };
    }

    protected abstract ALGO newAlgo(CONFIG config);

    protected abstract String taskName();

    @Override
    public final GraphAlgorithmFactory<ALGO, CONFIG> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return AlphaSimilarityProc.this.taskName();
            }

            @Override
            public ALGO build(
                Graph graph,
                CONFIG configuration,
                ProgressTracker progressTracker
            ) {
                removeGraph(username(), databaseId());
                return newAlgo(configuration);
            }
        };
    }

    @Override
    public ProcConfigParser<CONFIG> configParser() {
        return new AlphaSimilarityProcConfigParser<>(
            username(),
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
