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
package org.neo4j.graphalgo.triangle;

import org.jetbrains.annotations.Nullable;
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

public class TriangleCountStatsProc extends StatsProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountStatsProc.StatsResult, TriangleCountStreamConfig> {


    @Procedure(value = "gds.triangleCount.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.triangleCount.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    protected void validateConfigs(
        GraphCreateConfig graphCreateConfig, TriangleCountStreamConfig config
    ) {
        TriangleCountCompanion.validateConfigs(graphCreateConfig, config);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountStreamConfig> computeResult) {
        return TriangleCountCompanion.resultBuilder(
            new TriangleCountStatsBuilder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    @Override
    protected TriangleCountStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TriangleCountStreamConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<IntersectingTriangleCount, TriangleCountStreamConfig> algorithmFactory(
        TriangleCountStreamConfig config
    ) {
        return new IntersectingTriangleCountFactory<>();
    }

    public static class StatsResult {
        public final long createMillis;
        public final long computeMillis;
        public final long postProcessingMillis;
        public final long nodeCount;
        public final long triangleCount;
        public final double averageClusteringCoefficient;
        public final Map<String, Object> communityDistribution;
        public final Map<String, Object> configuration;

        public StatsResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long nodeCount,
            long triangleCount,
            double averageClusteringCoefficient,
            @Nullable Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodeCount = nodeCount;
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            this.triangleCount = triangleCount;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }
    }

    static class TriangleCountStatsBuilder extends TriangleCountCompanion.TriangleCountResultBuilder<StatsResult> {

        TriangleCountStatsBuilder(
            ProcedureCallContext callContext,
            AllocationTracker tracker
        ) {
            super(callContext, tracker);
        }

        @Override
        protected StatsResult buildResult() {
            return new StatsResult(
                createMillis,
                computeMillis,
                postProcessingDuration,
                nodeCount,
                triangleCount,
                averageClusteringCoefficient,
                communityHistogramOrNull(),
                config.toMap()
            );
        }
    }
}
