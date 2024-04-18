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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecStreamResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(
    name = "gds.node2vec.stream",
    aliases = "gds.beta.node2vec.stream",
    description = Node2VecCompanion.DESCRIPTION,
    executionMode = STREAM
)
public class Node2VecStreamSpec implements AlgorithmSpec<Node2Vec, Node2VecResult, Node2VecStreamConfig, Stream<Node2VecStreamResult>, Node2VecAlgorithmFactory<Node2VecStreamConfig>> {
    @Override
    public String name() {
        return "Node2VecStream";
    }

    @Override
    public Node2VecAlgorithmFactory<Node2VecStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<Node2VecStreamConfig> newConfigFunction() {
        return (__, userInput) -> Node2VecStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Node2Vec, Node2VecResult, Node2VecStreamConfig, Stream<Node2VecStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
