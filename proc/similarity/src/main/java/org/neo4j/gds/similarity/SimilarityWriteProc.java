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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.WriteRelationshipsProc;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;

import java.util.Collections;
import java.util.stream.Stream;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

public abstract class SimilarityWriteProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends WritePropertyConfig & WriteRelationshipConfig & AlgoBaseConfig> extends WriteRelationshipsProc<ALGO, ALGO_RESULT, SimilarityWriteResult, CONFIG> {

    public abstract String procedureName();

    @Override
    protected Stream<SimilarityWriteResult> write(AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
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

            var algorithm = computationResult.algorithm();
            var similarityGraphResult = similarityGraphResult(computationResult);
            var similarityGraph = similarityGraphResult.similarityGraph();
            // The relationships in the similarity graph refer to the node id space
            // of the graph store. Because of that, we must not use the similarity
            // graph itself to resolve the original node ids for a given source/target
            // id as this can lead to either assertion errors or to wrong original ids.
            // An exception is the topK graph where relationships refer to source/target
            // ids within the node id space of the similarity graph. Therefore it is
            // safe to use that graph for resolving original ids.
            var rootNodeMapping = similarityGraphResult.isTopKGraph()
                ? similarityGraph
                : computationResult.graphStore().nodes();

            SimilarityProc.SimilarityResultBuilder<SimilarityWriteResult> resultBuilder =
                SimilarityProc.resultBuilder(new SimilarityWriteResult.Builder(), computationResult, (ignore) -> similarityGraphResult);

            if (similarityGraph.relationshipCount() > 0) {
                String writeRelationshipType = config.writeRelationshipType();
                String writeProperty = config.writeProperty();

                runWithExceptionLogging(
                    procedureName() + " write-back failed",
                    () -> {
                        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                            var task = Tasks.leaf(
                                algoName() + " :: WriteRelationships",
                                similarityGraph.relationshipCount()
                            );
                            var progressTracker = new TaskProgressTracker(
                                task,
                                log,
                                RelationshipExporterBuilder.DEFAULT_WRITE_CONCURRENCY,
                                taskRegistryFactory
                            );
                            var exporter = relationshipExporterBuilder
                                .withIdMapping(rootNodeMapping)
                                .withGraph(similarityGraph)
                                .withTerminationFlag(algorithm.getTerminationFlag())
                                .withProgressTracker(progressTracker)
                                .build();

                            progressTracker.beginSubTask();
                            if (SimilarityProc.shouldComputeHistogram(callContext)) {
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
                            progressTracker.endSubTask();
                        }
                    }
                );
            }
            return Stream.of(resultBuilder.build());
        });
    }

    protected abstract SimilarityGraphResult similarityGraphResult(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult);
}
