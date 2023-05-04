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

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyDoubleNodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.localClusteringCoefficient.mutate", description = LocalClusteringCoefficientCompanion.DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class LocalClusteringCoefficientMutateSpec implements AlgorithmSpec<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateConfig, Stream<LocalClusteringCoefficientMutateResult>, LocalClusteringCoefficientFactory<LocalClusteringCoefficientMutateConfig>> {
    @Override
    public String name() {
        return "LocalClusteringCoefficientMutate";
    }

    @Override
    public LocalClusteringCoefficientFactory<LocalClusteringCoefficientMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new LocalClusteringCoefficientFactory<>();
    }

    @Override
    public NewConfigFunction<LocalClusteringCoefficientMutateConfig> newConfigFunction() {
        return (___, config) -> LocalClusteringCoefficientMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateConfig, Stream<LocalClusteringCoefficientMutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                computationResult.result()
                    .map(LocalClusteringCoefficient.Result::asNodeProperties)
                    .orElse(EmptyDoubleNodePropertyValues.INSTANCE)
            )),
            this::resultBuilder
        );
    }


    private AbstractResultBuilder<LocalClusteringCoefficientMutateResult> resultBuilder(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new LocalClusteringCoefficientMutateResult.Builder();

        computationResult.result()
            .ifPresent(result -> builder.withAverageClusteringCoefficient(result.averageClusteringCoefficient()));

        return builder;
    }
}
