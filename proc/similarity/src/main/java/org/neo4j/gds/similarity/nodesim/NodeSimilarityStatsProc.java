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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.gds.similarity.SimilarityStatsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.nodeSimilarity.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class NodeSimilarityStatsProc extends StatsProc<NodeSimilarity, NodeSimilarityResult, SimilarityStatsResult, NodeSimilarityStatsConfig> {

    @Procedure(name = "gds.nodeSimilarity.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<SimilarityStatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Procedure(value = "gds.nodeSimilarity.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodeSimilarityStatsConfig newConfig(String username, CypherMapWrapper config) {
        return NodeSimilarityStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<NodeSimilarity, NodeSimilarityStatsConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    protected AbstractResultBuilder<SimilarityStatsResult> resultBuilder(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        throw new UnsupportedOperationException("NodeSimilarity handles result building individually.");
    }

    @Override
    public Stream<SimilarityStatsResult> stats(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> computationResult) {
        return runWithExceptionLogging("Graph stats failed", () -> {
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

            SimilarityProc.SimilarityResultBuilder<SimilarityStatsResult> resultBuilder =
                SimilarityProc.withGraphsizeAndTimings(new SimilarityStatsResult.Builder(), computationResult, NodeSimilarityResult::graphResult);

            if (shouldComputeHistogram(executionContext().callContext())) {
                try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                    resultBuilder.withHistogram(computeHistogram(computationResult.result().graphResult().similarityGraph()));
                }
            }
            return Stream.of(resultBuilder.build());
        });
    }
}
