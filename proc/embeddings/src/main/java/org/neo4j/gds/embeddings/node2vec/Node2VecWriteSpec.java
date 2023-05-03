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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyFloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.node2vec.write", description = Node2VecCompanion.DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class Node2VecWriteSpec implements AlgorithmSpec<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig, Stream<WriteResult>, Node2VecAlgorithmFactory<Node2VecWriteConfig>> {
    @Override
    public String name() {
        return "Node2VecWrite";
    }

    @Override
    public Node2VecAlgorithmFactory<Node2VecWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<Node2VecWriteConfig> newConfigFunction() {
        return (__, userInput) -> Node2VecWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            this::nodePropertyList,
            name()
        );
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig> computationResult) {
        return List.of(
            NodeProperty.of(
                computationResult.config().writeProperty(),
                nodePropertyValues(computationResult)
            )
        );
    }

    @NotNull
    private static FloatArrayNodePropertyValues nodePropertyValues(ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig> computationResult) {
        return computationResult.result()
            .map(result -> (FloatArrayNodePropertyValues) new EmbeddingNodePropertyValues(result.embeddings()))
            .orElse(EmptyFloatArrayNodePropertyValues.INSTANCE);
    }

    private WriteResult.Builder resultBuilder(
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        var builder = new WriteResult.Builder();

        computeResult.result()
            .map(Node2VecModel.Result::lossPerIteration)
            .ifPresent(builder::withLossPerIteration);

        return builder;
    }

}
