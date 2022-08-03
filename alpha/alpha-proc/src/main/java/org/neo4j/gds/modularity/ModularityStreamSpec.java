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
package org.neo4j.gds.modularity;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.modularity.ModularityStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.modularity.stream", description = DESCRIPTION, executionMode = STREAM)
public class ModularityStreamSpec implements AlgorithmSpec<ModularityCalculator, List<CommunityModularity>, ModularityStreamConfig, Stream<ModularityResult>, ModularityCalculatorFactory> {
    @Override
    public String name() {
        return "ModularityStream";
    }

    @Override
    public ModularityCalculatorFactory algorithmFactory() {
        return new ModularityCalculatorFactory();
    }

    @Override
    public NewConfigFunction<ModularityStreamConfig> newConfigFunction() {
        return (__, userInput) -> ModularityStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<ModularityCalculator, List<CommunityModularity>, ModularityStreamConfig, Stream<ModularityResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> Optional.ofNullable(computationResult.result())
            .orElseGet(List::of)
            .stream()
            .map(ModularityResult::from);
    }
}
