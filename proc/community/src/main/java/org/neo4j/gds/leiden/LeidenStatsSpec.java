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
package org.neo4j.gds.leiden;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.leiden.LeidenStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.beta.leiden.stats", description = DESCRIPTION, executionMode = STREAM)
public class LeidenStatsSpec implements AlgorithmSpec<Leiden, LeidenResult, LeidenStatsConfig, Stream<StatsResult>, LeidenAlgorithmFactory<LeidenStatsConfig>> {
    @Override
    public String name() {
        return "LeidenStats";
    }

    @Override
    public LeidenAlgorithmFactory<LeidenStatsConfig> algorithmFactory() {
        return new LeidenAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LeidenStatsConfig> newConfigFunction() {
        return (__, config) -> LeidenStatsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Leiden, LeidenResult, LeidenStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var leidenResult = computationResult.result();
            if (leidenResult == null) {
                return Stream.empty();
            }

            var statsBuilder = new StatsResult.StatsBuilder(
                executionContext.callContext(),
                computationResult.config().concurrency()
            );

            var statsResult = statsBuilder
                .withLevels(leidenResult.ranLevels())
                .withDidConverge(leidenResult.didConverge())
                .withModularity(leidenResult.modularity())
                .withModularities(Arrays.stream(leidenResult.modularities())
                    .boxed()
                    .collect(Collectors.toList())).withCommunityFunction(leidenResult.communitiesFunction())
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(computationResult.config())
                .build();

            return Stream.of(statsResult);
        };
    }
}
