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
package org.neo4j.gds.k1coloring;

import org.neo4j.gds.StatsComputationResultConsumer;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.k1coloring.K1ColoringSpecificationHelper.K1_COLORING_DESCRIPTION;

@GdsCallable(name = "gds.beta.k1coloring.stats", description = K1_COLORING_DESCRIPTION, executionMode = STATS)
public class K1ColoringStatsSpecification implements AlgorithmSpec<K1Coloring, HugeLongArray, K1ColoringStatsConfig, Stream<K1ColoringStatsResult>, K1ColoringFactory<K1ColoringStatsConfig>> {

    @Override
    public String name() {
        return "K1ColoringStats";
    }

    @Override
    public K1ColoringFactory<K1ColoringStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new K1ColoringFactory<>();
    }

    @Override
    public NewConfigFunction<K1ColoringStatsConfig> newConfigFunction() {
        return (__, userInput) -> K1ColoringStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<K1Coloring, HugeLongArray, K1ColoringStatsConfig, Stream<K1ColoringStatsResult>> computationResultConsumer() {
        return new StatsComputationResultConsumer<>(K1ColoringStatsSpecification::resultBuilder);
    }

    private static AbstractResultBuilder<K1ColoringStatsResult> resultBuilder(
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        K1ColoringStatsResult.Builder builder = new K1ColoringStatsResult.Builder(
            executionContext.returnColumns(),
            computeResult.config().concurrency()
        );
        return K1ColoringSpecificationHelper.resultBuilder(builder, computeResult, executionContext.returnColumns());
    }
}
