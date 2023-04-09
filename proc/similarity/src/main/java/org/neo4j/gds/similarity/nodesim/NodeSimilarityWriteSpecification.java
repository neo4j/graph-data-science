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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityWriteConsumer;
import org.neo4j.gds.similarity.SimilarityWriteResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.gds.similarity.nodesim.NodeSimilarityProc.NODE_SIMILARITY_DESCRIPTION;

@GdsCallable(name = "gds.nodeSimilarity.write", description = NODE_SIMILARITY_DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class NodeSimilarityWriteSpecification implements AlgorithmSpec<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig, Stream<SimilarityWriteResult>, NodeSimilarityFactory<NodeSimilarityWriteConfig>> {
    @Override
    public String name() {
        return "NodeSimilarityWrite";
    }

    @Override
    public NodeSimilarityFactory<NodeSimilarityWriteConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<NodeSimilarityWriteConfig> newConfigFunction() {
        return (__, userInput) -> NodeSimilarityWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig, Stream<SimilarityWriteResult>> computationResultConsumer() {
        return new SimilarityWriteConsumer<>(
            (computationResult) -> new SimilarityWriteResult.Builder(),
            (computationResult) -> computationResult.result()
                .map(NodeSimilarityResult::graphResult)
                .orElseGet(() -> new SimilarityGraphResult(computationResult.graph(), 0, false)),
            name()
        );
    }
}
