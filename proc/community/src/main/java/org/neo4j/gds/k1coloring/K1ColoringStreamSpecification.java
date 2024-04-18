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
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringStreamResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.k1coloring.K1ColoringSpecificationHelper.K1_COLORING_DESCRIPTION;

@GdsCallable(name = "gds.k1coloring.stream",
             aliases = {"gds.beta.k1coloring.stream"},
             description = K1_COLORING_DESCRIPTION,
             executionMode = STREAM)
public class K1ColoringStreamSpecification implements AlgorithmSpec<K1Coloring, K1ColoringResult, K1ColoringStreamConfig, Stream<K1ColoringStreamResult>, K1ColoringAlgorithmFactory<K1ColoringStreamConfig>> {

    @Override
    public String name() {
        return "K1ColoringStream";
    }

    @Override
    public K1ColoringAlgorithmFactory<K1ColoringStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new K1ColoringAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<K1ColoringStreamConfig> newConfigFunction() {
        return (__, userInput) -> K1ColoringStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<K1Coloring, K1ColoringResult, K1ColoringStreamConfig, Stream<K1ColoringStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
