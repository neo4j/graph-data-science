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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.beta.node2vec.stream", description = Node2VecCompanion.DESCRIPTION, executionMode = STREAM)
public class Node2VecStreamSpec  implements AlgorithmSpec<Node2Vec, Node2VecModel.Result, Node2VecStreamConfig, Stream<StreamResult>, Node2VecAlgorithmFactory<Node2VecStreamConfig>> {
    @Override
    public String name() {
        return "Node2VecStream";
    }

    @Override
    public Node2VecAlgorithmFactory<Node2VecStreamConfig> algorithmFactory() {
        return new Node2VecAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<Node2VecStreamConfig> newConfigFunction() {
        return (__, userInput) -> Node2VecStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Node2Vec, Node2VecModel.Result, Node2VecStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> computationResult.result()
            .map(result -> {
                var graph = computationResult.graph();
                var nodePropertyValues = new EmbeddingNodePropertyValues(result.embeddings());
                return LongStream
                    .range(IdMap.START_NODE_ID, graph.nodeCount())
                    .filter(nodePropertyValues::hasValue)
                    .mapToObj(nodeId -> new StreamResult(
                        graph.toOriginalNodeId(nodeId),
                        nodePropertyValues.floatArrayValue(nodeId)
                    ));
            })
            .orElseGet(Stream::empty);
    }
}
