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

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.modularityoptimization.ModularityOptimizationSpecificationHelper.MODULARITY_OPTIMIZATION_DESCRIPTION;

@GdsCallable(name = "gds.beta.modularityOptimization.stream", description = MODULARITY_OPTIMIZATION_DESCRIPTION, executionMode = STREAM)
public class ModularityOptimizationStreamSpecification implements AlgorithmSpec<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStreamConfig, Stream<ModularityOptimizationStreamResult>, ModularityOptimizationFactory<ModularityOptimizationStreamConfig>> {

    @Override
    public String name() {
        return "ModularityOptimizationStream";
    }

    @Override
    public ModularityOptimizationFactory<ModularityOptimizationStreamConfig> algorithmFactory() {
        return new ModularityOptimizationFactory<>();
    }

    @Override
    public NewConfigFunction<ModularityOptimizationStreamConfig> newConfigFunction() {
        return (__, userInput) -> ModularityOptimizationStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStreamConfig, Stream<ModularityOptimizationStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) ->
            runWithExceptionLogging("Result streaming failed", executionContext.log(), () -> {
                if (computationResult.isGraphEmpty()) {
                    return Stream.empty();
                }

                var graph = computationResult.graph();
                var config = computationResult.config();
                var nodePropertyValues = nodeProperties(computationResult);
                return LongStream
                    .range(IdMap.START_NODE_ID, graph.nodeCount())
                    .filter(nodePropertyValues::hasValue)
                    .mapToObj(nodeId -> new ModularityOptimizationStreamResult(
                        graph.toOriginalNodeId(nodeId),
                        nodePropertyValues.longValue(nodeId)
                    ));
            });
    }

    private NodePropertyValues nodeProperties(
        ComputationResult<ModularityOptimization, ModularityOptimizationResult, ModularityOptimizationStreamConfig> computationResult
    ) {
        return CommunityProcCompanion.nodeProperties(
            computationResult.config(),
            computationResult.result()
                .map(ModularityOptimizationResult::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE)
        );
    }
}
