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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.pathfinding.ResultBuilder;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class SteinerTreeResultBuilderForStreamMode extends ResultBuilder<SteinerTreeStreamConfig, SteinerTreeResult, Stream<SteinerTreeStreamResult>> {
    @Override
    public Stream<SteinerTreeStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        SteinerTreeStreamConfig configuration,
        Optional<SteinerTreeResult> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) return Stream.of();

        var steinerTreeResult = result.get();

        var sourceNodeId = configuration.sourceNode();
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

                    return new SteinerTreeStreamResult(originalNodeId, parentNodeId, cost);
                }
            );
    }
}
