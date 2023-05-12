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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.fastRP.stats", description = "Random Projection produces node embeddings via the fastrp algorithm", executionMode = STATS)
public class FastRPStatsSpec implements AlgorithmSpec<FastRP, FastRP.FastRPResult, FastRPStatsConfig, Stream<StatsResult>, FastRPFactory<FastRPStatsConfig>> {
    @Override
    public String name() {
        return "FastRPStats";
    }

    @Override
    public FastRPFactory<FastRPStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new FastRPFactory<>();
    }

    @Override
    public NewConfigFunction<FastRPStatsConfig> newConfigFunction() {
        return (__, userInput) -> FastRPStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FastRP, FastRP.FastRPResult, FastRPStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Stats call failed",
            executionContext.log(),
            () -> Stream.of(
                new StatsResult.Builder()
                    .withPreProcessingMillis(computationResult.preProcessingMillis())
                    .withComputeMillis(computationResult.computeMillis())
                    .withNodeCount(computationResult.graph().nodeCount())
                    .withConfig(computationResult.config())
                    .build()
            )
        );
    }
}
