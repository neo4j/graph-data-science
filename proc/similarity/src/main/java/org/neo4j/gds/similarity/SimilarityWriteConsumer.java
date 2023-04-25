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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

public class SimilarityWriteConsumer<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT extends SimilarityWriteResult,
    CONFIG extends WritePropertyConfig & WriteRelationshipConfig & AlgoBaseConfig> implements ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, Stream<PROC_RESULT>> {

    private final Function<ComputationResult<ALGO, ALGO_RESULT, CONFIG>, SimilarityResultBuilder<PROC_RESULT>> resultBuilderFunction;
    private final Function<ComputationResult<ALGO, ALGO_RESULT, CONFIG>, SimilarityGraphResult> similarityGraphFunction;

    private final String name;

    public SimilarityWriteConsumer(
        Function<ComputationResult<ALGO, ALGO_RESULT, CONFIG>, SimilarityResultBuilder<PROC_RESULT>> resultBuilderFunction,
        Function<ComputationResult<ALGO, ALGO_RESULT, CONFIG>, SimilarityGraphResult> similarityGraphFunction,
        String name
    ) {
        this.resultBuilderFunction = resultBuilderFunction;
        this.similarityGraphFunction = similarityGraphFunction;
        this.name = name;
    }

    @Override
    public Stream<PROC_RESULT> consume(
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        return runWithExceptionLogging(
            name + " write-back failed",
            executionContext.log(),
            () -> {
                CONFIG config = computationResult.config();
                var resultBuilder = resultBuilderFunction.apply(computationResult);
                if (computationResult.isGraphEmpty()) {
                    return Stream.of(resultBuilder.withConfig(config).build());
                }

                var algorithm = computationResult.algorithm();
                var similarityGraphResult = similarityGraphFunction.apply(computationResult);
                var similarityGraph = similarityGraphResult.similarityGraph();
                // The relationships in the similarity graph refer to the node id space
                // of the graph store. Because of that, we must not use the similarity
                // graph itself to resolve the original node ids for a given source/target
                // id as this can lead to either assertion errors or to wrong original ids.
                // An exception is the topK graph where relationships refer to source/target
                // ids within the node id space of the similarity graph. Therefore, it is
                // safe to use that graph for resolving original ids.
                var rootIdMap = similarityGraphResult.isTopKGraph()
                    ? similarityGraph
                    : computationResult.graphStore().nodes();

                SimilarityProc.withGraphsizeAndTimings(
                    resultBuilder,
                    computationResult,
                    (ignore) -> similarityGraphResult
                );

                if (similarityGraph.relationshipCount() > 0) {
                    String writeRelationshipType = config.writeRelationshipType();
                    String writeProperty = config.writeProperty();

                    try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                        var progressTracker = new TaskProgressTracker(
                            RelationshipExporter.baseTask(name, similarityGraph.relationshipCount()),
                            executionContext.log(),
                            RelationshipExporterBuilder.DEFAULT_WRITE_CONCURRENCY,
                            executionContext.taskRegistryFactory()
                        );
                        var relationshipExporterBuilder = Optional
                            .ofNullable(executionContext.relationshipExporterBuilder())
                            .orElseThrow();
                        var exporter = relationshipExporterBuilder
                            .withIdMappingOperator(rootIdMap::toOriginalNodeId)
                            .withGraph(similarityGraph)
                            .withTerminationFlag(algorithm.getTerminationFlag())
                            .withProgressTracker(progressTracker)
                            .withArrowConnectionInfo(config.arrowConnectionInfo())
                            .build();

                        if (SimilarityProc.shouldComputeHistogram(executionContext.returnColumns())) {
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

                return Stream.of(resultBuilder.build());
            }
        );
    }
}
