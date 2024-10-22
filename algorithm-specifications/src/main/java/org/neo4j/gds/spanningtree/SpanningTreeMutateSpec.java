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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeMutateResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(
    name = "gds.spanningTree.mutate",
    aliases = {"gds.beta.spanningTree.mutate"},
    description = Constants.SPANNING_TREE_DESCRIPTION,
    executionMode = MUTATE_RELATIONSHIP
)
public class SpanningTreeMutateSpec implements AlgorithmSpec<Prim, SpanningTree, SpanningTreeMutateConfig, Stream<SpanningTreeMutateResult>, SpanningTreeAlgorithmFactory<SpanningTreeMutateConfig>> {

    @Override
    public String name() {
        return "SpanningTreeMutate";
    }

    @Override
    public SpanningTreeAlgorithmFactory<SpanningTreeMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SpanningTreeMutateConfig> newConfigFunction() {
        return (__, config) -> SpanningTreeMutateConfig.of(config);

    }

    public ComputationResultConsumer<Prim, SpanningTree, SpanningTreeMutateConfig, Stream<SpanningTreeMutateResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
