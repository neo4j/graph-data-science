/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.nodesim;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StatsProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.computeHistogram;
import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityStatsProc extends StatsProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsProc.StatsResult, NodeSimilarityStatsConfig> {

    @Procedure(name = "gds.nodeSimilarity.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
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
    protected AlgorithmFactory<NodeSimilarity, NodeSimilarityStatsConfig> algorithmFactory(NodeSimilarityStatsConfig config) {
        return new NodeSimilarityFactory<>();
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> computeResult) {
        throw new UnsupportedOperationException("NodeSimilarity handles result building individually.");
    }

    @Override
    public Stream<StatsResult> stats(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> computationResult) {
        NodeSimilarityStatsConfig config = computationResult.config();

        if (computationResult.isGraphEmpty()) {
            return Stream.of(
                new StatsResult(
                    computationResult.createMillis(),
                    0,
                    0,
                    0,
                    Collections.emptyMap(),
                    config.toMap()
                )
            );
        }

        NodeSimilarityProc.NodeSimilarityResultBuilder<StatsResult> resultBuilder =
            NodeSimilarityProc.resultBuilder(new StatsResult.Builder(), computationResult);

        if (shouldComputeHistogram(callContext)) {
            try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                resultBuilder.withHistogram(computeHistogram(computationResult.result().graphResult().similarityGraph()));
            }
        }
        return Stream.of(resultBuilder.build());
    }

    public static final class StatsResult {

        public long createMillis;
        public long computeMillis;
        public long postProcessingMillis;
        public long nodesCompared;
        public Map<String, Object> similarityDistribution;
        public Map<String, Object> configuration;

        StatsResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long nodesCompared,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration

        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.similarityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends NodeSimilarityProc.NodeSimilarityResultBuilder<StatsResult> {

            @Override
            public StatsResult build() {
                return new StatsResult(
                    createMillis,
                    computeMillis,
                    postProcessingMillis,
                    nodesCompared,
                    distribution(),
                    config.toMap()
                );
            }
        }
    }
}
