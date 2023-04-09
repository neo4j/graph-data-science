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
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.beta.influenceMaximization.celf.mutate", description = DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class CELFMutateSpec implements AlgorithmSpec<CELF, LongDoubleScatterMap, InfluenceMaximizationMutateConfig, Stream<MutateResult>, CELFAlgorithmFactory<InfluenceMaximizationMutateConfig>> {
    @Override
    public String name() {
        return "CELFStream";
    }

    @Override
    public CELFAlgorithmFactory<InfluenceMaximizationMutateConfig> algorithmFactory() {
        return new CELFAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<InfluenceMaximizationMutateConfig> newConfigFunction() {
        return (__, userInput) -> InfluenceMaximizationMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<CELF, LongDoubleScatterMap, InfluenceMaximizationMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        MutatePropertyComputationResultConsumer.MutateNodePropertyListFunction<CELF, LongDoubleScatterMap, InfluenceMaximizationMutateConfig> mutateConfigNodePropertyListFunction =
            computationResult -> {
                var celfSeedSet = computationResult.result()
                    .orElseGet(() -> new LongDoubleScatterMap(0));

                var nodeCount = computationResult.graph().nodeCount();
                var celfSeedSetNodeProperty = ImmutableNodeProperty.of(
                    computationResult.config().mutateProperty(),
                    new CelfNodeProperties(celfSeedSet, nodeCount)
                );
                return List.of(celfSeedSetNodeProperty);
            };
        return new MutatePropertyComputationResultConsumer<>(
            mutateConfigNodePropertyListFunction,
            this::resultBuilder
        );
    }

    @NotNull
    private AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<CELF, LongDoubleScatterMap, InfluenceMaximizationMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var celfSpreadSetValues = computationResult.result()
            .orElseGet(() -> new LongDoubleScatterMap(0))
            .values;
        return MutateResult.builder()
            .withTotalSpread(Arrays.stream(celfSpreadSetValues).sum())
            .withNodeCount(computationResult.graph().nodeCount())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config());
    }

}
