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
package org.neo4j.gds.procedures.embeddings;

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.embeddings.EmbeddingNodePropertyValues;
import org.neo4j.gds.algorithms.embeddings.specificfields.Node2VecSpecificFields;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecMutateResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecStreamResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecWriteResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

class Node2VecComputationalResultTransformer {

    static Stream<Node2VecStreamResult> toStreamResult(
        StreamComputationResult<Node2VecResult> computationResult
    ) {
        return computationResult.result().map(node2VecResult -> {
            var graph = computationResult.graph();
            var nodePropertyValues = new EmbeddingNodePropertyValues(node2VecResult.embeddings());
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(nodeId -> new Node2VecStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.floatArrayValue(nodeId)));

        }).orElseGet(Stream::empty);
    }

    static Node2VecMutateResult toMutateResult(NodePropertyMutateResult<Node2VecSpecificFields> mutateResult) {

        return new Node2VecMutateResult(
            mutateResult.algorithmSpecificFields().nodeCount(),
            mutateResult.nodePropertiesWritten(),
            mutateResult.preProcessingMillis(),
            mutateResult.computeMillis(),
            mutateResult.mutateMillis(),
            mutateResult.configuration().toMap(),
            mutateResult.algorithmSpecificFields().lossPerIteration()
        );
    }

    static Node2VecWriteResult toWriteResult(NodePropertyWriteResult<Node2VecSpecificFields> mutateResult) {

        return new Node2VecWriteResult(
            mutateResult.algorithmSpecificFields().nodeCount(),
            mutateResult.nodePropertiesWritten(),
            mutateResult.preProcessingMillis(),
            mutateResult.computeMillis(),
            mutateResult.writeMillis(),
            mutateResult.configuration().toMap(),
            mutateResult.algorithmSpecificFields().lossPerIteration()
        );
    }
}
