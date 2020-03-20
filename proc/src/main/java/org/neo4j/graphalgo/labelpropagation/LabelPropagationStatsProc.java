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
package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StatsProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationStatsProc extends StatsProc<LabelPropagation, LabelPropagation, LabelPropagationStatsProc.StatsResult, LabelPropagationStreamConfig> {

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
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationStreamConfig> computeResult) {
        return LabelPropagationProc.resultBuilder(
            new StatsResult.Builder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    @Override
    protected LabelPropagationStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<LabelPropagation, LabelPropagationStreamConfig> algorithmFactory(
        LabelPropagationStreamConfig config
    ) {
        return new LabelPropagationFactory<>(config);
    }

    public static class StatsResult {

        public long createMillis;
        public long computeMillis;
        public long postProcessingMillis;
        public long communityCount;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> communityDistribution;
        public Map<String, Object> configuration;

        StatsResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long communityCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.communityCount = communityCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<StatsResult> {

            Builder(
                ProcedureCallContext context,
                AllocationTracker tracker
            ) {
                super(
                    context,
                    tracker
                );
            }

            @Override
            protected StatsResult buildResult() {
                return new StatsResult(
                    createMillis,
                    computeMillis,
                    postProcessingDuration,
                    maybeCommunityCount.orElse(-1L),
                    ranIterations,
                    didConverge,
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
