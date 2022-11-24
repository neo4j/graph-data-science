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
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeAlgorithmFactory;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.alpha.steinerTree.stats", description = SteinerTreeStatsProc.DESCRIPTION, executionMode = STATS)

public class SteinerTreeStatsSpec implements AlgorithmSpec<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStatsConfig, Stream<StatsResult>, SteinerTreeAlgorithmFactory<SteinerTreeStatsConfig>> {

    @Override
    public String name() {
        return "SteinerTreeStats";
    }

    @Override
    public SteinerTreeAlgorithmFactory<SteinerTreeStatsConfig> algorithmFactory() {
        return new SteinerTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SteinerTreeStatsConfig> newConfigFunction() {
        return (__, config) -> SteinerTreeStatsConfig.of(config);

    }

    public ComputationResultConsumer<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStatsConfig, Stream<StatsResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            var steinerAlgorithm = computationResult.algorithm();
            var steinerTreeResult = computationResult.result();
            SteinerTreeStatsConfig config = computationResult.config();

            var builder = new StatsResult.Builder();

            if (graph.isEmpty()) {
                graph.release();
                return Stream.of(builder.build());
            }

            builder.withEffectiveNodeCount(steinerTreeResult.effectiveNodeCount());
            builder.withTotalWeight(steinerTreeResult.totalCost());
            builder.withEffectiveTargetNodesCount(steinerTreeResult.effectiveTargetNodesCount());

            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            builder.withConfig(config);
            return Stream.of(builder.build());
        };
    }
}
