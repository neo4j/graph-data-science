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
package org.neo4j.gds.louvain;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.louvain.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class LouvainStatsProc extends StatsProc<Louvain, LouvainResult, LouvainStatsProc.StatsResult, LouvainStatsConfig> {

    @Procedure(value = "gds.louvain.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Procedure(value = "gds.louvain.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<Louvain, LouvainResult, LouvainStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return LouvainProc.resultBuilder(
            new StatsResult.Builder(executionContext.callContext(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    protected LouvainStatsConfig newConfig(String username, CypherMapWrapper config) {
        return LouvainStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Louvain, LouvainStatsConfig> algorithmFactory() {
        return new LouvainFactory<>();
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final double modularity;
        public final List<Double> modularities;
        public final long ranLevels;
        public final long communityCount;
        public final Map<String, Object> communityDistribution;

        StatsResult(
            double modularity,
            List<Double> modularities,
            long ranLevels,
            long communityCount,
            Map<String, Object> communityDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            Map<String, Object> configuration
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
            this.modularity = modularity;
            this.modularities = modularities;
            this.ranLevels = ranLevels;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
        }

        static class Builder extends LouvainProc.LouvainResultBuilder<StatsResult> {

            Builder(ProcedureCallContext context, int concurrency) {
                super(context, concurrency);
            }

            @Override
            protected StatsResult buildResult() {
                return new StatsResult(
                    modularity,
                    Arrays.stream(modularities).boxed().collect(Collectors.toList()),
                    levels,
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    preProcessingMillis,
                    computeMillis,
                    postProcessingDuration,
                    config.toMap()
                );
            }
        }

    }
}
