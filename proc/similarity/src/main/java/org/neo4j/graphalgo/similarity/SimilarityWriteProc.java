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
package org.neo4j.graphalgo.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.config.WriteRelationshipConfig;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.RelationshipExporter;

import java.util.Collections;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.graphalgo.similarity.SimilarityProc.shouldComputeHistogram;

public abstract class SimilarityWriteProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends WritePropertyConfig & WriteRelationshipConfig & AlgoBaseConfig> extends WriteProc<ALGO, ALGO_RESULT, SimilarityWriteResult, CONFIG> {

    public abstract String procedureName();

    @Override
    protected Stream<SimilarityWriteResult> write(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        return runWithExceptionLogging("Graph write failed", () -> {
            CONFIG config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new SimilarityWriteResult(
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

            var rootNodeMapping = computationResult.graphStore().nodes();
            var algorithm = computationResult.algorithm();
            var similarityGraphResult = similarityGraphResult(computationResult);
            var similarityGraph = similarityGraphResult.similarityGraph();

            SimilarityProc.SimilarityResultBuilder<SimilarityWriteResult> resultBuilder =
                SimilarityProc.resultBuilder(new SimilarityWriteResult.Builder(), computationResult, (ignore) -> similarityGraphResult);

            if (similarityGraph.relationshipCount() > 0) {
                String writeRelationshipType = config.writeRelationshipType();
                String writeProperty = config.writeProperty();

                runWithExceptionLogging(
                    procedureName() + " write-back failed",
                    () -> {
                        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                            RelationshipExporter exporter = RelationshipExporter
                                .of(
                                    TransactionContext.of(api, procedureTransaction),
                                    rootNodeMapping,
                                    similarityGraph,
                                    algorithm.getTerminationFlag()
                                )
                                .withLog(log)
                                .build();
                            if (shouldComputeHistogram(callContext)) {
                                DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
                                exporter.write(
                                    writeRelationshipType,
                                    writeProperty,
                                    (node1, node2, similarity) -> {
                                        histogram.recordValue(similarity);
                                        return true;
                                    }
                                );
                                resultBuilder.withHistogram(histogram);
                            } else {
                                exporter.write(writeRelationshipType, writeProperty);
                            }
                        }
                    }
                );
            }
            return Stream.of(resultBuilder.build());
        });
    }

    protected abstract SimilarityGraphResult similarityGraphResult(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult);
}
