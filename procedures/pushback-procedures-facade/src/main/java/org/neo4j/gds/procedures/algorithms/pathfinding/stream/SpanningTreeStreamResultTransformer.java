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
package org.neo4j.gds.procedures.algorithms.pathfinding.stream;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.spanningtree.SpanningTree;

import java.util.stream.LongStream;
import java.util.stream.Stream;

class SpanningTreeStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<SpanningTree>, Stream<SpanningTreeStreamResult>> {

    private final Graph graph;
    private final long sourceNodeId;

    SpanningTreeStreamResultTransformer(Graph graph, long sourceNodeId) {
        this.graph = graph;
        this.sourceNodeId = sourceNodeId;
    }

    @Override
    public Stream<SpanningTreeStreamResult> apply(TimedAlgorithmResult<SpanningTree> timedAlgorithmResult) {
        var spanningTree = timedAlgorithmResult.result();
        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .filter(nodeId -> spanningTree.parent(nodeId) >= 0 || sourceNodeId == graph.toOriginalNodeId(nodeId))
            .mapToObj(nodeId -> {
                var originalId = graph.toOriginalNodeId(nodeId);
                return new SpanningTreeStreamResult(
                    originalId,
                    (sourceNodeId == originalId) ? sourceNodeId : graph.toOriginalNodeId(spanningTree.parent(nodeId)),
                    spanningTree.costToParent(nodeId)
                );
            });
    }
}
