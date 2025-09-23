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
package org.neo4j.gds.procedures.algorithms.embeddings.stream;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingsStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.stream.LongStream;
import java.util.stream.Stream;

class FastRPStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<FastRPResult>, Stream<DefaultNodeEmbeddingsStreamResult>> {
    private final Graph graph;

    FastRPStreamResultTransformer(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> apply(TimedAlgorithmResult<FastRPResult> algorithmResult) {
        var fastRPResult = algorithmResult.result();
        var nodePropertyValues = NodePropertyValuesAdapter.adapt(fastRPResult.embeddings());
        return LongStream
            .range(IdMap.START_NODE_ID, nodePropertyValues.nodeCount())
            .filter(nodePropertyValues::hasValue)
            .mapToObj(nodeId -> DefaultNodeEmbeddingsStreamResult.create(
                graph.toOriginalNodeId(nodeId),
                nodePropertyValues.floatArrayValue(nodeId)
            ));
    }
}
