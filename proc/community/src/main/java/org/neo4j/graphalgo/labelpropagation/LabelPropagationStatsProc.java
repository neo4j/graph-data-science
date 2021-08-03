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
package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.labelpropagation.LabelPropagation;
import org.neo4j.gds.labelpropagation.LabelPropagationFactory;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationStatsProc extends StatsProc<LabelPropagation, LabelPropagation, LabelPropagationStatsProc.StatsResult, LabelPropagationStatsConfig> {

    @Procedure(value = "gds.labelPropagation.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.labelPropagation.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationStatsConfig> computeResult) {
        return LabelPropagationProc.resultBuilder(
            new StatsResult.Builder(callContext, computeResult.config().concurrency(), allocationTracker()),
            computeResult
        );
    }

    @Override
    protected LabelPropagationStatsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationStatsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<LabelPropagation, LabelPropagationStatsConfig> algorithmFactory() {
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
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            Map<String, Object> configuration
        ) {
            super(createMillis, computeMillis, postProcessingMillis, configuration);
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
        }

        static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<StatsResult> {

            Builder(ProcedureCallContext context, int concurrency, AllocationTracker tracker) {
                super(context, concurrency, tracker);
            }

            @Override
            protected StatsResult buildResult() {
                return new StatsResult(
                    ranIterations,
                    didConverge,
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    createMillis,
                    computeMillis,
                    postProcessingDuration,
                    config.toMap()
                );
            }
        }
    }
}
