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
package org.neo4j.gds.similarity.ann;

import org.HdrHistogram.DoubleHistogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.similarity.ApproxNearestNeighborsAlgorithm;
import org.neo4j.gds.impl.similarity.ApproximateNearestNeighborsConfig;
import org.neo4j.gds.impl.similarity.Computations;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.gds.impl.similarity.SimilarityInput;
import org.neo4j.gds.results.SimilarityResult;
import org.neo4j.gds.similarity.SimilarityExporter;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.gds.similarity.ann.ApproxNearestNeighborsProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.alpha.ml.ann.write", description = DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class ApproxNearestNeighborsWriteProc extends ApproxNearestNeighborsProc<ApproxSimilaritySummaryResult> {

    @Procedure(name = "gds.alpha.ml.ann.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<ApproxSimilaritySummaryResult> annWrite(
        @Name(value = "configuration") Map<String, Object> configuration
    ) {
        var computationResult = compute(
            SIMILARITY_FAKE_GRAPH_NAME,
            configuration
        );
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ComputationResultConsumer<ApproxNearestNeighborsAlgorithm<SimilarityInput>, SimilarityAlgorithmResult, ApproximateNearestNeighborsConfig, Stream<ApproxSimilaritySummaryResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            ApproximateNearestNeighborsConfig config = computationResult.config();
            SimilarityAlgorithmResult result = computationResult.result();
            assert result != null;

            if (result.isEmpty()) {
                return emptyStreamResult(result, config, computationResult.algorithm());
            }

            return writeAndAggregateANNResults(result, config, computationResult.algorithm());
        };
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
        DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        SimilarityExporter similarityExporter = new SimilarityExporter(
            TransactionContext.of(api, procedureTransaction),
            config.writeRelationshipType(),
            config.writeProperty(),
            algorithm.getTerminationFlag()
        );
        similarityExporter.export(algoResult.stream().peek(recorder), config.writeBatchSize());

        return Stream.of(
            ApproxSimilaritySummaryResult.from(
                algoResult.nodes(),
                similarityPairs.get(),
                algoResult.computations().map(Computations::count).orElse(-1L),
                config.writeRelationshipType(),
                config.writeProperty(),
                algorithm.iterations(),
                histogram
            )
        );
    }
}
