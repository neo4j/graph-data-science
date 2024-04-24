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
package org.neo4j.gds.applications.graphstorecatalog;

import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.ParallelUtil;

import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class StreamRelationshipsApplication {
    Stream<TopologyResult> compute(GraphStore graphStore, GraphStreamRelationshipsConfig configuration) {
        var relationshipTypesAndGraphs = configuration.relationshipTypeIdentifiers(graphStore).stream()
            .map(relationshipType -> Tuples.pair(relationshipType.name(), graphStore.getGraph(relationshipType)))
            .collect(Collectors.toList());

        return ParallelUtil.parallelStream(
            LongStream.range(0, graphStore.nodeCount()),
            configuration.concurrency(),
            nodeStream -> nodeStream
                .boxed()
                .flatMap(nodeId -> relationshipTypesAndGraphs.stream().flatMap(graphAndRelationshipType -> {
                    var relationshipType = graphAndRelationshipType.getOne();
                    Graph graph = graphAndRelationshipType.getTwo();

                    var originalSourceId = graph.toOriginalNodeId(nodeId);

                    return graph
                        .streamRelationships(nodeId, Double.NaN)
                        .map(relationshipCursor -> new TopologyResult(
                            originalSourceId,
                            graph.toOriginalNodeId(relationshipCursor.targetId()),
                            relationshipType
                        ));
                }))
        );
    }
}
