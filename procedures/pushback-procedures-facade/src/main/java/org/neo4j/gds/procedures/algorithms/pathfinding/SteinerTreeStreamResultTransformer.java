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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SteinerTreeStreamResultTransformer implements ResultTransformer<SteinerTreeResult, Stream<SpanningTreeStreamResult>> {

    private final Graph graph;
    private final long sourceNodeId;

    SteinerTreeStreamResultTransformer(Graph graph, long sourceNodeId) {
        this.graph = graph;
        this.sourceNodeId = sourceNodeId;
    }

    @Override
    public Stream<SpanningTreeStreamResult> apply(SteinerTreeResult steinerTreeResult) {

        var parents = steinerTreeResult.parentArray();
        var costs = steinerTreeResult.relationshipToParentCost();

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .filter(nodeId -> parents.get(nodeId) != ShortestPathsSteinerAlgorithm.PRUNED)
            .mapToObj(nodeId -> {
                    var originalNodeId = graph.toOriginalNodeId(nodeId);
                    var parentNodeId = (sourceNodeId == originalNodeId)
                        ? sourceNodeId
                        : graph.toOriginalNodeId(parents.get(nodeId));
                    var cost = costs.get(nodeId);

                    return new SpanningTreeStreamResult(originalNodeId, parentNodeId, cost);
                }
            );
    }
}
