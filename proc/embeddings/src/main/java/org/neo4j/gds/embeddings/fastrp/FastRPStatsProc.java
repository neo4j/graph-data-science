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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.fastrp.FastRPCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class FastRPStatsProc extends StatsProc<FastRP, FastRP.FastRPResult, FastRPStatsProc.StatsResult, FastRPStatsConfig> {

    @Procedure(value = "gds.fastRP.stats", mode = READ)
    @Description("Random Projection produces node embeddings via the fastrp algorithm")
    public Stream<StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPStatsConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stats(computationResult);
    }
    @Procedure(value = "gds.fastRP.stats.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }


    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<FastRP, FastRP.FastRPResult, FastRPStatsConfig> computeResult) {
        return new StatsResult.Builder();
    }

    @Override
    protected FastRPStatsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return FastRPStatsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<FastRP, FastRPStatsConfig> algorithmFactory() {
        return new FastRPFactory<>();
    }

    @SuppressWarnings("unused")
    public static final class StatsResult {

        public final long nodeCount;
        public final long createMillis;
        public final long computeMillis;
        public final Map<String, Object> configuration;

        StatsResult(
            long nodeCount,
            long createMillis,
            long computeMillis,
            Map<String, Object> config
        ) {
            this.nodeCount = nodeCount;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.configuration = config;
        }

        static final class Builder extends AbstractResultBuilder<StatsResult> {

            @Override
            public StatsResult build() {
                return new StatsResult(
                    nodeCount,
                    createMillis,
                    computeMillis,
                    config.toMap()
                );
            }
        }
    }
}
