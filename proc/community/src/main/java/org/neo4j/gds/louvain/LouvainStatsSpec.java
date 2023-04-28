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
package org.neo4j.gds.louvain;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.louvain.LouvainConstants.DESCRIPTION;

@GdsCallable(name = "gds.louvain.stats", description = DESCRIPTION, executionMode = STREAM)
public class LouvainStatsSpec implements AlgorithmSpec<Louvain, LouvainResult, LouvainStatsConfig, Stream<StatsResult>, LouvainAlgorithmFactory<LouvainStatsConfig>> {
    @Override
    public String name() {
        return "LouvainStats";
    }

    @Override
    public LouvainAlgorithmFactory<LouvainStatsConfig> algorithmFactory() {
        return new LouvainAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LouvainStatsConfig> newConfigFunction() {
        return (__, config) -> LouvainStatsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Louvain, LouvainResult, LouvainStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {

            var louvainStatsResultBuilder = new LouvainStatsResultsBuilder(
                executionContext.returnColumns(),
                computationResult.config().concurrency()
            );

            louvainStatsResultBuilder.withConfig(computationResult.config());

            if (computationResult.result().isEmpty()) return Stream.of(louvainStatsResultBuilder.build());

            var louvainResult = computationResult.result().get();
            louvainStatsResultBuilder.withCommunityFunction(louvainResult.communitiesFunction());
            louvainStatsResultBuilder.withComputeMillis(computationResult.computeMillis());
            louvainStatsResultBuilder.withLevels(louvainResult.ranLevels());
            louvainStatsResultBuilder.withModularities(louvainResult.modularities());
            louvainStatsResultBuilder.withModularity(louvainResult.modularity());
            louvainStatsResultBuilder.withNodeCount(computationResult.graph().nodeCount());
            louvainStatsResultBuilder.withPreProcessingMillis(computationResult.preProcessingMillis());

            return Stream.of(louvainStatsResultBuilder.build());
        };
    }
}
