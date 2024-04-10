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
package org.neo4j.gds.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.MutateNodePropertyListFunction;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.centrality.celf.CELFMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;

@GdsCallable(
    name = "gds.influenceMaximization.celf.mutate",
    aliases = {"gds.beta.influenceMaximization.celf.mutate"},
    description = DESCRIPTION,
    executionMode = MUTATE_NODE_PROPERTY
)
public class CELFMutateSpec implements AlgorithmSpec<CELF, CELFResult, InfluenceMaximizationMutateConfig, Stream<CELFMutateResult>, CELFAlgorithmFactory<InfluenceMaximizationMutateConfig>> {
    @Override
    public String name() {
        return "CELFMutate";
    }

    @Override
    public CELFAlgorithmFactory<InfluenceMaximizationMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new CELFAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<InfluenceMaximizationMutateConfig> newConfigFunction() {
        return (__, userInput) -> InfluenceMaximizationMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<CELF, CELFResult, InfluenceMaximizationMutateConfig, Stream<CELFMutateResult>> computationResultConsumer() {
        MutateNodePropertyListFunction<CELF, CELFResult, InfluenceMaximizationMutateConfig> mutateConfigNodePropertyListFunction =
            computationResult -> {
                var celfResult = computationResult.result()
                    .orElseGet(() -> new CELFResult(new LongDoubleScatterMap(0)));

                var nodeCount = computationResult.graph().nodeCount();
                var celfSeedSetNodeProperty = ImmutableNodeProperty.of(
                    computationResult.config().mutateProperty(),
                    new CELFNodeProperties(celfResult.seedSetNodes(), nodeCount)
                );
                return List.of(celfSeedSetNodeProperty);
            };
        return new MutatePropertyComputationResultConsumer<>(
            mutateConfigNodePropertyListFunction,
            this::resultBuilder
        );
    }

    @NotNull
    private AbstractResultBuilder<CELFMutateResult> resultBuilder(
        ComputationResult<CELF, CELFResult, InfluenceMaximizationMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var celfResult = computationResult.result();
        return CELFMutateResult.builder()
            .withTotalSpread(celfResult.map(res -> res.totalSpread()).orElse(0D))
            .withNodeCount(computationResult.graph().nodeCount())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config());
    }

}
