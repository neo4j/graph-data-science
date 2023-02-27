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
package org.neo4j.gds.paths.singlesource.bellmanford;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordAlgorithmFactory;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.bellmanFord.stats", description = BellmanFord.DESCRIPTION, executionMode = STATS)
public class BellmanFordStatsSpec implements AlgorithmSpec<BellmanFord, BellmanFordResult, BellmanFordStatsConfig, Stream<BellmanFordStatsResult>, BellmanFordAlgorithmFactory<BellmanFordStatsConfig>> {
    @Override
    public String name() {
        return "BellmanFordStats";
    }

    @Override
    public BellmanFordAlgorithmFactory<BellmanFordStatsConfig> algorithmFactory() {
        return new BellmanFordAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<BellmanFordStatsConfig> newConfigFunction() {
        return (username, configuration) -> BellmanFordStatsConfig.of(configuration);
    }

    @Override
    public ComputationResultConsumer<BellmanFord, BellmanFordResult, BellmanFordStatsConfig, Stream<BellmanFordStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) ->
            Stream.of(new BellmanFordStatsResult(
                computationResult.preProcessingMillis(),
                computationResult.computeMillis(),
                0,
                computationResult.config().toMap(),
                computationResult.result().containsNegativeCycle()
            ));
    }
}
