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
package org.neo4j.gds.similarity.filterednodesim;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.gds.similarity.SimilarityWriteResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.nodeSimilarity.filtered.write", description = DESCRIPTION, executionMode = ExecutionMode.WRITE_RELATIONSHIP)
public class FilteredNodeSimilarityWriteSpec implements AlgorithmSpec<
    NodeSimilarity,
    NodeSimilarityResult,
    FilteredNodeSimilarityWriteConfig,
    Stream<SimilarityWriteResult>,
    FilteredNodeSimilarityFactory<FilteredNodeSimilarityWriteConfig>
    > {

    @Override
    public String name() {
        return "FilteredNodeSimilarityWrite";
    }

    @Override
    public FilteredNodeSimilarityFactory<FilteredNodeSimilarityWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new FilteredNodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredNodeSimilarityWriteConfig> newConfigFunction() {
        return (__, userInput) -> FilteredNodeSimilarityWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityWriteConfig, Stream<SimilarityWriteResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging("Graph write failed", executionContext.log(), () -> {
            var config = computationResult.config();
            var resultBuilder = new SimilarityWriteResult.Builder();

            if (computationResult.result().isEmpty()) {
                return Stream.of(resultBuilder.withConfig(config).build());
            }

            var algorithm = computationResult.algorithm();
            var similarityGraphResult = computationResult.result().get().graphResult();
            var similarityGraph = similarityGraphResult.similarityGraph();
            // The relationships in the similarity graph refer to the node id space
            // of the graph store. Because of that, we must not use the similarity
            // graph itself to resolve the original node ids for a given source/target
            // id as this can lead to either assertion errors or to wrong original ids.
            // An exception is the topK graph where relationships refer to source/target
            // ids within the node id space of the similarity graph. Therefore it is
            // safe to use that graph for resolving original ids.
            var rootIdMap = similarityGraphResult.isTopKGraph()
                ? similarityGraph
                : computationResult.graphStore().nodes();

            SimilarityProc.withGraphsizeAndTimings(resultBuilder, computationResult, (ignore) -> similarityGraphResult);

            if (similarityGraph.relationshipCount() > 0) {
                String writeRelationshipType = config.writeRelationshipType();
                String writeProperty = config.writeProperty();

                runWithExceptionLogging(
                    name() + " write-back failed",
                    executionContext.log(),
                    () -> {
                        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                            var progressTracker = new TaskProgressTracker(
                                RelationshipExporter.baseTask(name(), similarityGraph.relationshipCount()),
                                executionContext.log(),
                                RelationshipExporterBuilder.DEFAULT_WRITE_CONCURRENCY,
                                executionContext.taskRegistryFactory()
                            );
                            var exporter = executionContext.relationshipExporterBuilder()
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
                        // Have to return something..
                        return 1;
                    }
                );
            }
            return Stream.of(resultBuilder.build());
        });
    }
}
