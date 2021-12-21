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
package org.neo4j.gds.embeddings.fastrp;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.fastrp.FastRPCompanion.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.fastRP.stats", description = "Random Projection produces node embeddings via the fastrp algorithm", executionMode = STATS)
public class FastRPStatsProc extends StatsProc<FastRP, FastRP.FastRPResult, FastRPStatsProc.StatsResult, FastRPStatsConfig> {

    @Procedure(value = "gds.fastRP.stats", mode = READ)
    @Description("Random Projection produces node embeddings via the fastrp algorithm")
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPStatsConfig> computationResult = compute(
            graphName,
            configuration
        );
        return stats(computationResult);
    }

    @Procedure(value = "gds.fastRP.stats.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }


    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new StatsResult.Builder();
    }

    @Override
    protected FastRPStatsConfig newConfig(String username, CypherMapWrapper config) {
        return FastRPStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<FastRP, FastRPStatsConfig> algorithmFactory() {
        return new FastRPFactory<>();
    }

    @SuppressWarnings("unused")
    public static final class StatsResult {

        public final long nodeCount;
        public final long preProcessingMillis;
        public final long computeMillis;
        public final Map<String, Object> configuration;

        StatsResult(
            long nodeCount,
            long preProcessingMillis,
            long computeMillis,
            Map<String, Object> config
        ) {
            this.nodeCount = nodeCount;
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.configuration = config;
        }

        static final class Builder extends AbstractResultBuilder<StatsResult> {

            @Override
            public StatsResult build() {
                return new StatsResult(
                    nodeCount,
                    preProcessingMillis,
                    computeMillis,
                    config.toMap()
                );
            }
        }
    }
}
