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

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityStatsProc extends NodeSimilarityBaseProc<NodeSimilarityStatsConfig> {

    @Procedure(name = "gds.nodeSimilarity.stats", mode = Mode.WRITE)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        AlgoBaseProc.ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStatsConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        return stats(result);
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

        NodeSimilarityResult result = computationResult.result();
        SimilarityGraphResult similarityGraphResult = result.maybeGraphResult().get();
        Graph similarityGraph = similarityGraphResult.similarityGraph();

        StatsResultBuilder resultBuilder = new StatsResultBuilder();
        resultBuilder
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withRelationshipsWritten(similarityGraph.relationshipCount());
        resultBuilder.withCreateMillis(computationResult.createMillis());
        resultBuilder.withComputeMillis(computationResult.computeMillis());
        resultBuilder.withConfig(config);

        boolean shouldComputeHistogram = callContext
            .outputFields()
            .anyMatch(s -> s.equalsIgnoreCase("similarityDistribution"));
        if (shouldComputeHistogram) {
            try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                resultBuilder.withHistogram(computeHistogram(similarityGraph));
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
    }

    static class StatsResultBuilder extends AbstractResultBuilder<StatsResult> {

        private long nodesCompared = 0L;

        private long postProcessingMillis = -1L;

        private Optional<DoubleHistogram> maybeHistogram = Optional.empty();

        public StatsResultBuilder withNodesCompared(long nodesCompared) {
            this.nodesCompared = nodesCompared;
            return this;
        }

        StatsResultBuilder withHistogram(DoubleHistogram histogram) {
            this.maybeHistogram = Optional.of(histogram);
            return this;
        }

        void setPostProcessingMillis(long postProcessingMillis) {
            this.postProcessingMillis = postProcessingMillis;
        }

        ProgressTimer timePostProcessing() {
            return ProgressTimer.start(this::setPostProcessingMillis);
        }

        private Map<String, Object> distribution() {
            if (maybeHistogram.isPresent()) {
                DoubleHistogram definitelyHistogram = maybeHistogram.get();
                return MapUtil.map(
                    "min", definitelyHistogram.getMinValue(),
                    "max", definitelyHistogram.getMaxValue(),
                    "mean", definitelyHistogram.getMean(),
                    "stdDev", definitelyHistogram.getStdDeviation(),
                    "p1", definitelyHistogram.getValueAtPercentile(1),
                    "p5", definitelyHistogram.getValueAtPercentile(5),
                    "p10", definitelyHistogram.getValueAtPercentile(10),
                    "p25", definitelyHistogram.getValueAtPercentile(25),
                    "p50", definitelyHistogram.getValueAtPercentile(50),
                    "p75", definitelyHistogram.getValueAtPercentile(75),
                    "p90", definitelyHistogram.getValueAtPercentile(90),
                    "p95", definitelyHistogram.getValueAtPercentile(95),
                    "p99", definitelyHistogram.getValueAtPercentile(99),
                    "p100", definitelyHistogram.getValueAtPercentile(100)
                );
            }
            return Collections.emptyMap();
        }

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
