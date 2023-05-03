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
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
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

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.node2vec.mutate", description = Node2VecCompanion.DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class Node2VecMutateSpec implements AlgorithmSpec<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig, Stream<MutateResult>, Node2VecAlgorithmFactory<Node2VecMutateConfig>> {
    @Override
    public String name() {
        return "Node2VecMutate";
    }

    @Override
    public Node2VecAlgorithmFactory<Node2VecMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<Node2VecMutateConfig> newConfigFunction() {
        return (__, userInput) -> Node2VecMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            this::nodePropertyList,
            this::resultBuilder
        );
    }

    private List<NodeProperty> nodePropertyList(
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig> computationResult
    ) {
        return List.of(
            NodeProperty.of(
                computationResult.config().mutateProperty(),
                nodePropertyValues(computationResult)
            )
        );
    }

    @NotNull
    private static FloatArrayNodePropertyValues nodePropertyValues(ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig> computationResult) {
        return computationResult.result()
            .map(result -> (FloatArrayNodePropertyValues) new EmbeddingNodePropertyValues(result.embeddings()))
            .orElse(EmptyFloatArrayNodePropertyValues.INSTANCE);
    }

    private MutateResult.Builder resultBuilder(
        ComputationResult<Node2Vec, Node2VecModel.Result, Node2VecMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        var builder = new MutateResult.Builder();
        computeResult.result().ifPresent(result -> {
            builder.withLossPerIteration(result.lossPerIteration());
        });

        return builder;
    }

}
