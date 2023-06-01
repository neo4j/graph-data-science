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

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.nodeproperties.ConsecutiveLongNodePropertyValues;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.modularityoptimization.ModularityOptimizationSpecificationHelper.MODULARITY_OPTIMIZATION_DESCRIPTION;

@GdsCallable(name = "gds.beta.modularityOptimization.mutate", description = MODULARITY_OPTIMIZATION_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ModularityOptimizationMutateSpecification implements AlgorithmSpec<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationMutateConfig, Stream<ModularityOptimizationMutateResult>, ModularityOptimizationFactory<ModularityOptimizationMutateConfig>> {

    @Override
    public String name() {
        return "ModularityOptimizationMutate";
    }

    @Override
    public ModularityOptimizationFactory<ModularityOptimizationMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ModularityOptimizationFactory<>();
    }

    @Override
    public NewConfigFunction<ModularityOptimizationMutateConfig> newConfigFunction() {
        return (__, userInput) -> ModularityOptimizationMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationMutateConfig, Stream<ModularityOptimizationMutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            this::nodePropertyList,
            this::resultBuilder
        );
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationMutateConfig> computationResult) {
        var resultCommunities = computationResult.result()
            .map(ModularityOptimizationResult::asNodeProperties)
            .orElse(EmptyLongNodePropertyValues.INSTANCE);
        NodePropertyValues nodePropertyValues;
        if (computationResult.config().consecutiveIds()) {
            nodePropertyValues = new ConsecutiveLongNodePropertyValues(resultCommunities);
        } else {
            nodePropertyValues = resultCommunities;
        }
        return List.of(ImmutableNodeProperty.of(computationResult.config().mutateProperty(), nodePropertyValues));
    }

    private AbstractResultBuilder<ModularityOptimizationMutateResult> resultBuilder(
        ComputationResult<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return ModularityOptimizationSpecificationHelper.resultBuilder(
            new ModularityOptimizationMutateResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }
}
