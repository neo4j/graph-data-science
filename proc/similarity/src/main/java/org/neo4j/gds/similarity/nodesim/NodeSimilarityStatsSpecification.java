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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.gds.similarity.SimilarityResultBuilder;
import org.neo4j.gds.similarity.SimilarityStatsResult;

import java.util.Collections;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.gds.similarity.nodesim.NodeSimilarityProc.NODE_SIMILARITY_DESCRIPTION;

@GdsCallable(name = "gds.nodeSimilarity.stats", description = NODE_SIMILARITY_DESCRIPTION, executionMode = STATS)
public class NodeSimilarityStatsSpecification implements AlgorithmSpec<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig, Stream<SimilarityStatsResult>, NodeSimilarityFactory<NodeSimilarityStatsConfig>> {
    @Override
    public String name() {
        return "NodeSimilarityStats";
    }

    @Override
    public NodeSimilarityFactory<NodeSimilarityStatsConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<NodeSimilarityStatsConfig> newConfigFunction() {
        return (__, userInput) -> NodeSimilarityStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig, Stream<SimilarityStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            return runWithExceptionLogging("Graph stats failed", executionContext.log(), () -> {
                NodeSimilarityStatsConfig config = computationResult.config();

                if (computationResult.isGraphEmpty()) {
                    return Stream.of(
                        new SimilarityStatsResult(
                            computationResult.preProcessingMillis(),
                            0,
                            0,
                            0,
                            0,
                            Collections.emptyMap(),
                            config.toMap()
                        )
                    );
                }

                SimilarityResultBuilder<SimilarityStatsResult> resultBuilder =
                    SimilarityProc.withGraphsizeAndTimings(new SimilarityStatsResult.Builder(), computationResult, NodeSimilarityResult::graphResult);

                if (shouldComputeHistogram(executionContext.returnColumns())) {
                    try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                        resultBuilder.withHistogram(computeHistogram(computationResult.result().graphResult().similarityGraph()));
                    }
                }
                return Stream.of(resultBuilder.build());
            });
        };
    }
}
