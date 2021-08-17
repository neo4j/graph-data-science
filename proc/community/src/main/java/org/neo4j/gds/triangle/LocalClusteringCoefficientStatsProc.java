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
package org.neo4j.gds.triangle;


import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.config.GraphCreateConfigValidations.validateIsUndirectedGraph;
import static org.neo4j.gds.triangle.LocalClusteringCoefficientCompanion.warnOnGraphWithParallelRelationships;
import static org.neo4j.procedure.Mode.READ;

public class LocalClusteringCoefficientStatsProc extends StatsProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsProc.StatsResult, LocalClusteringCoefficientStatsConfig> {


    @Procedure(value = "gds.localClusteringCoefficient.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.localClusteringCoefficient.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected void validateConfigsBeforeLoad(GraphCreateConfig graphCreateConfig, LocalClusteringCoefficientStatsConfig config) {
        validateIsUndirectedGraph(graphCreateConfig, config);
        warnOnGraphWithParallelRelationships(graphCreateConfig, config, log);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsConfig> computeResult) {
        return LocalClusteringCoefficientCompanion.resultBuilder(
            new LocalClusteringCoefficientStatsBuilder(callContext, computeResult.config().concurrency(), allocationTracker()),
            computeResult
        );
    }

    @Override
    protected LocalClusteringCoefficientStatsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LocalClusteringCoefficientStatsConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<LocalClusteringCoefficient, LocalClusteringCoefficientStatsConfig> algorithmFactory() {
        return new LocalClusteringCoefficientFactory<>();
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final double averageClusteringCoefficient;
        public final long nodeCount;

        StatsResult(
            double averageClusteringCoefficient,
            long nodeCount,
            long createMillis,
            long computeMillis,
            Map<String, Object> configuration
        ) {
            // post-processing is instant for LCC
            super(createMillis, computeMillis, 0L, configuration);
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            this.nodeCount = nodeCount;
        }
    }

    static class LocalClusteringCoefficientStatsBuilder extends LocalClusteringCoefficientCompanion.ResultBuilder<StatsResult> {

        LocalClusteringCoefficientStatsBuilder(ProcedureCallContext callContext, int concurrency, AllocationTracker tracker) {
            super(callContext, concurrency, tracker);
        }

        @Override
        protected StatsResult buildResult() {
            return new StatsResult(
                averageClusteringCoefficient,
                nodeCount,
                createMillis,
                computeMillis,
                config.toMap()
            );
        }
    }
}
