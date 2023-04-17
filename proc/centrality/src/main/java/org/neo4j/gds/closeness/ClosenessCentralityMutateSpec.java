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
package org.neo4j.gds.closeness;

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.beta.closeness.ClosenessCentrality;
import org.neo4j.gds.beta.closeness.ClosenessCentralityFactory;
import org.neo4j.gds.beta.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.beta.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
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

import static org.neo4j.gds.beta.closeness.ClosenessCentrality.CLOSENESS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.closeness.mutate", description = CLOSENESS_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ClosenessCentralityMutateSpec implements AlgorithmSpec<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateConfig,Stream<MutateResult>, ClosenessCentralityFactory<ClosenessCentralityMutateConfig>> {

    @Override
    public String name() {
            return "ClosenessCentralityMutate";
    }

    @Override
    public ClosenessCentralityFactory<ClosenessCentralityMutateConfig> algorithmFactory() {
        return new ClosenessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<ClosenessCentralityMutateConfig> newConfigFunction() {
        return (___, config) -> ClosenessCentralityMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                computationResult.result()
                    .map(ClosenessCentralityResult::centralities)
                    .orElseGet(() -> HugeDoubleArray.newArray(0))
                    .asNodeProperties()
            )),
            this::resultBuilder
        );
    }
    private AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new MutateResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );

        computationResult.result().ifPresent(result -> builder.withCentralityFunction(result.centralities()::get));

        return builder;
    }

}
