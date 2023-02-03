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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.labelPropagation.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class LabelPropagationStatsProc extends StatsProc<LabelPropagation, LabelPropagationResult, LabelPropagationStatsProc.StatsResult, LabelPropagationStatsConfig> {

    @Procedure(value = "gds.labelPropagation.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Procedure(value = "gds.labelPropagation.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<LabelPropagation, LabelPropagationResult, LabelPropagationStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return LabelPropagationProc.resultBuilder(
            new StatsResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    protected LabelPropagationStatsConfig newConfig(String username, CypherMapWrapper config) {
        return LabelPropagationStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<LabelPropagation, LabelPropagationStatsConfig> algorithmFactory() {
        return new LabelPropagationFactory<>();
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final long ranIterations;
        public final boolean didConverge;
        public final long communityCount;
        public final Map<String, Object> communityDistribution;

        StatsResult(
            long ranIterations,
            boolean didConverge,
            long communityCount,
            Map<String, Object> communityDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            Map<String, Object> configuration
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
        }

        static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<StatsResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected StatsResult buildResult() {
                return new StatsResult(
                    ranIterations,
                    didConverge,
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
