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
package org.neo4j.gds.triangle;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.localClusteringCoefficient.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class LocalClusteringCoefficientStatsSpec implements AlgorithmSpec<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsConfig, Stream<LocalClusteringCoefficientStatsResult>, LocalClusteringCoefficientFactory<LocalClusteringCoefficientStatsConfig>> {

    @Override
    public String name() {
        return "LocalClusteringCoefficientStats";
    }

    @Override
    public LocalClusteringCoefficientFactory<LocalClusteringCoefficientStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new LocalClusteringCoefficientFactory<>();
    }

    @Override
    public NewConfigFunction<LocalClusteringCoefficientStatsConfig> newConfigFunction() {
        return (___, config) -> LocalClusteringCoefficientStatsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsConfig, Stream<LocalClusteringCoefficientStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var builder = new LocalClusteringCoefficientStatsResult.Builder();
            computationResult.result()
                .ifPresent(result ->
                    builder
                        .withAverageClusteringCoefficient(result.averageClusteringCoefficient())
                );

            builder
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(computationResult.config());

            return Stream.of(builder.build());
        };
    }


}
