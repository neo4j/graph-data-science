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
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.maxflow.FlowResult;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MaxFlowResultBuilderForStreamMode implements StreamResultBuilder<FlowResult, MaxFlowStreamResult> {

    @Override
    public Stream<MaxFlowStreamResult> build(Graph graph, GraphStore graphStore, Optional<FlowResult> flowResult) {
        return flowResult.map(flowResult1 -> {
            var result = flowResult1.flow();
            var size = result.size();
            return LongStream.range(0, size).mapToObj(i -> {
                var flowRelationship = result.get(i);
                return new MaxFlowStreamResult(
                    graph.toOriginalNodeId(flowRelationship.sourceId()),
                    graph.toOriginalNodeId(flowRelationship.targetId()),
                    flowRelationship.flow()
                );
            });
        }).orElse(Stream.empty());
    }
}
