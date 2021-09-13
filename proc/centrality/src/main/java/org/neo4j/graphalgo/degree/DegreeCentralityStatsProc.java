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
package org.neo4j.graphalgo.degree;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StatsProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.result.AbstractCentralityResultBuilder;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.graphalgo.results.StandardStatsResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.degree.DegreeCentralityProc.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class DegreeCentralityStatsProc extends StatsProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStatsProc.StatsResult, DegreeCentralityStatsConfig> {

    @Procedure(value = "gds.degree.stats", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<DegreeCentralityStatsProc.StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.degree.stats.estimate", mode = READ)
    @Description(DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected DegreeCentralityStatsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return DegreeCentralityStatsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<DegreeCentrality, DegreeCentralityStatsConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStatsConfig> computationResult) {
        return DegreeCentralityProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStatsConfig> computeResult) {
        return DegreeCentralityProc.resultBuilder(
            new DegreeCentralityStatsProc.StatsResult.Builder(callContext, computeResult.config().concurrency()),
            computeResult
        );
    }

    public static class StatsResult extends StandardStatsResult {

        public final Map<String, Object> centralityDistribution;

        StatsResult(
            @Nullable Map<String, Object> centralityDistribution,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            Map<String, Object> configuration
        ) {
            super(createMillis, computeMillis, postProcessingMillis, configuration);
            this.centralityDistribution = centralityDistribution;
        }

        static final class Builder extends AbstractCentralityResultBuilder<DegreeCentralityStatsProc.StatsResult> {
            protected Builder(ProcedureCallContext callContext, int concurrency) {
                super(callContext, concurrency);
            }

            @Override
            public DegreeCentralityStatsProc.StatsResult buildResult() {
                return new DegreeCentralityStatsProc.StatsResult(
                    centralityHistogram,
                    createMillis,
                    computeMillis,
                    postProcessingMillis,
                    config.toMap()
                );
            }
        }
    }
}
