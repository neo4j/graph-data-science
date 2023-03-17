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
package org.neo4j.gds.closeness;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.beta.closeness.ClosenessCentrality;
import org.neo4j.gds.beta.closeness.ClosenessCentralityResult;
import org.neo4j.gds.beta.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.closeness.ClosenessCentrality.CLOSENESS_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ClosenessCentralityStatsProc extends StatsProc<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStatsProc.StatsResult, ClosenessCentralityStatsConfig> {
    @Procedure(value = "gds.beta.closeness.stats", mode = READ)
    @Description(CLOSENESS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Override
    public AlgorithmFactory<?, ClosenessCentrality, ClosenessCentralityStatsConfig> algorithmFactory() {
        return ClosenessCentralityProc.algorithmFactory();
    }

    @Override
    protected ClosenessCentralityStatsConfig newConfig(String username, CypherMapWrapper config) {
        return ClosenessCentralityStatsConfig.of(config);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return ClosenessCentralityProc.resultBuilder(
            new StatsResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final Map<String, Object> centralityDistribution;

        StatsResult(
            @Nullable Map<String, Object> centralityDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            Map<String, Object> configuration
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
            this.centralityDistribution = centralityDistribution;
        }

        static final class Builder extends AbstractCentralityResultBuilder<StatsResult> {
            private Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            public StatsResult buildResult() {
                return new StatsResult(
                    centralityHistogram,
                    preProcessingMillis,
                    computeMillis,
                    postProcessingMillis,
                    config.toMap()
                );
            }
        }
    }

}
