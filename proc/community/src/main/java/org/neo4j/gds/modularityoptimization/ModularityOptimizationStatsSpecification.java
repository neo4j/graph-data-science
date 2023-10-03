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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.procedures.community.modularityoptimization.ModularityOptimizationStatsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.modularityoptimization.ModularityOptimizationSpecificationHelper.MODULARITY_OPTIMIZATION_DESCRIPTION;

@GdsCallable(
    name = "gds.modularityOptimization.stats", description = MODULARITY_OPTIMIZATION_DESCRIPTION, executionMode = STATS
)
public class ModularityOptimizationStatsSpecification implements AlgorithmSpec<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStatsConfig, Stream<ModularityOptimizationStatsResult>, ModularityOptimizationFactory<ModularityOptimizationStatsConfig>> {

    @Override
    public String name() {
        return "ModularityOptimizationStats";
    }

    @Override
    public ModularityOptimizationFactory<ModularityOptimizationStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ModularityOptimizationFactory<>();
    }

    @Override
    public NewConfigFunction<ModularityOptimizationStatsConfig> newConfigFunction() {
        return (__, userInput) -> ModularityOptimizationStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStatsConfig, Stream<ModularityOptimizationStatsResult>> computationResultConsumer() {
        return  null;  //it's all in facade
    }


}
