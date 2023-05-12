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
package org.neo4j.gds.paths.steiner;


import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeAlgorithmFactory;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.beta.steinerTree.stats", description = SteinerTreeStatsProc.DESCRIPTION, executionMode = STATS)

public class SteinerTreeStatsSpec implements AlgorithmSpec<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStatsConfig, Stream<StatsResult>, SteinerTreeAlgorithmFactory<SteinerTreeStatsConfig>> {

    @Override
    public String name() {
        return "SteinerTreeStats";
    }

    @Override
    public SteinerTreeAlgorithmFactory<SteinerTreeStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SteinerTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SteinerTreeStatsConfig> newConfigFunction() {
        return (__, config) -> SteinerTreeStatsConfig.of(config);

    }

    public ComputationResultConsumer<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStatsConfig, Stream<StatsResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            var builder = new StatsResult.Builder();

            if (computationResult.result().isEmpty()) {
                return Stream.of(builder.build());
            }

            var steinerTreeResult = computationResult.result().get();

            builder
                .withEffectiveNodeCount(steinerTreeResult.effectiveNodeCount())
                .withTotalWeight(steinerTreeResult.totalCost())
                .withEffectiveTargetNodesCount(steinerTreeResult.effectiveTargetNodesCount())
                .withComputeMillis(computationResult.computeMillis())
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withConfig(computationResult.config());
            return Stream.of(builder.build());
        };
    }
}
