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
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MaxFlowStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.stream.LongStream;
import java.util.stream.Stream;

class MaxFlowStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<FlowResult>, Stream<MaxFlowStreamResult>> {

    private final Graph graph;

    MaxFlowStreamResultTransformer(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Stream<MaxFlowStreamResult> apply(TimedAlgorithmResult<FlowResult> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result().flow();
        var size = result.size();
        return LongStream.range(0, size).mapToObj(i -> {
            var flowRelationship = result.get(i);
            return new MaxFlowStreamResult(
                graph.toOriginalNodeId(flowRelationship.sourceId()),
                graph.toOriginalNodeId(flowRelationship.targetId()),
                flowRelationship.flow()
            );
        });
    }

}
