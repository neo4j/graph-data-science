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
package org.neo4j.gds.embeddings.fastrp;

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.fastRP.mutate", description = FastRPCompanion.DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class FastRPMutateSpec implements AlgorithmSpec<FastRP, FastRP.FastRPResult, FastRPMutateConfig, Stream<MutateResult>, FastRPFactory<FastRPMutateConfig>> {
    @Override
    public String name() {
        return "FastRPMutate";
    }

    @Override
    public FastRPFactory<FastRPMutateConfig> algorithmFactory() {
        return new FastRPFactory<>();
    }

    @Override
    public NewConfigFunction<FastRPMutateConfig> newConfigFunction() {
        return (__, userInput) -> FastRPMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FastRP, FastRP.FastRPResult, FastRPMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            (computationResult) -> List.of(
                NodeProperty.of(
                    computationResult.config().mutateProperty(),
                    FastRPCompanion.nodeProperties(computationResult)
                )
            ),
            this::resultBuilder
        );
    }

    private MutateResult.Builder resultBuilder(
        ComputationResult<FastRP, FastRP.FastRPResult, FastRPMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder();
    }
}
