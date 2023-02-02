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


import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.localClusteringCoefficient.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class LocalClusteringCoefficientStatsProc extends StatsProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsProc.StatsResult, LocalClusteringCoefficientStatsConfig> {

    @Procedure(value = "gds.localClusteringCoefficient.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Procedure(value = "gds.localClusteringCoefficient.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public ValidationConfiguration<LocalClusteringCoefficientStatsConfig> validationConfig(ExecutionContext executionContext) {
        return LocalClusteringCoefficientCompanion.getValidationConfig(executionContext.log());
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return LocalClusteringCoefficientCompanion.resultBuilder(
            new LocalClusteringCoefficientStatsBuilder(
                executionContext.callContext(),
                computeResult.config().concurrency()
            ),
            computeResult
        );
    }

    @Override
    protected LocalClusteringCoefficientStatsConfig newConfig(String username, CypherMapWrapper config) {
        return LocalClusteringCoefficientStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<LocalClusteringCoefficient, LocalClusteringCoefficientStatsConfig> algorithmFactory() {
        return new LocalClusteringCoefficientFactory<>();
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final double averageClusteringCoefficient;
        public final long nodeCount;

        StatsResult(
            double averageClusteringCoefficient,
            long nodeCount,
            long preProcessingMillis,
            long computeMillis,
            Map<String, Object> configuration
        ) {
            // post-processing is instant for LCC
            super(preProcessingMillis, computeMillis, 0L, configuration);
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            this.nodeCount = nodeCount;
        }
    }

    static class LocalClusteringCoefficientStatsBuilder extends LocalClusteringCoefficientCompanion.ResultBuilder<StatsResult> {

        LocalClusteringCoefficientStatsBuilder(
            ProcedureCallContext callContext,
            int concurrency
        ) {
            super(callContext, concurrency);
        }

        @Override
        protected StatsResult buildResult() {
            return new StatsResult(
                averageClusteringCoefficient,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                config.toMap()
            );
        }
    }
}
