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
package org.neo4j.graphalgo.similarity.nodesim;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityFactory;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsConfig;
import org.neo4j.gds.StatsProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.similarity.SimilarityProc;
import org.neo4j.graphalgo.similarity.SimilarityStatsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.graphalgo.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityStatsProc extends StatsProc<NodeSimilarity, NodeSimilarityResult, SimilarityStatsResult, NodeSimilarityStatsConfig> {

    @Procedure(name = "gds.nodeSimilarity.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<SimilarityStatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.nodeSimilarity.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeSimilarityStatsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeSimilarityStatsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<NodeSimilarity, NodeSimilarityStatsConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    protected AbstractResultBuilder<SimilarityStatsResult> resultBuilder(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> computeResult) {
        throw new UnsupportedOperationException("NodeSimilarity handles result building individually.");
    }

    @Override
    public Stream<SimilarityStatsResult> stats(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> computationResult) {
        return runWithExceptionLogging("Graph stats failed", () -> {
            NodeSimilarityStatsConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new SimilarityStatsResult(
                        computationResult.createMillis(),
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
                SimilarityProc.resultBuilder(new SimilarityStatsResult.Builder(), computationResult, NodeSimilarityResult::graphResult);

            if (shouldComputeHistogram(callContext)) {
                try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                    resultBuilder.withHistogram(computeHistogram(computationResult.result().graphResult().similarityGraph()));
                }
            }
            return Stream.of(resultBuilder.build());
        });
    }
}
