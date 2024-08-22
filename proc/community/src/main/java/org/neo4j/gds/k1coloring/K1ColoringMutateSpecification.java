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

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringMutateResult;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.k1coloring.K1ColoringSpecificationHelper.K1_COLORING_DESCRIPTION;

@GdsCallable(name = "gds.k1coloring.mutate",
             aliases = { "gds.beta.k1coloring.mutate"},
             description = K1_COLORING_DESCRIPTION,
             executionMode = MUTATE_NODE_PROPERTY)
public class K1ColoringMutateSpecification implements AlgorithmSpec<K1Coloring, K1ColoringResult, K1ColoringMutateConfig, Stream<K1ColoringMutateResult>, K1ColoringAlgorithmFactory<K1ColoringMutateConfig>> {

    @Override
    public String name() {
        return "K1ColoringMutate";
    }

    @Override
    public K1ColoringAlgorithmFactory<K1ColoringMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new K1ColoringAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<K1ColoringMutateConfig> newConfigFunction() {
        return (__, userInput) -> K1ColoringMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<K1Coloring, K1ColoringResult, K1ColoringMutateConfig, Stream<K1ColoringMutateResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
