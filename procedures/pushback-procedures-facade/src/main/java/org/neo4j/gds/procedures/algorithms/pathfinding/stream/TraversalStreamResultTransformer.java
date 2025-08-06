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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFactoryFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.TraversalStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.stream.Stream;

class TraversalStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<HugeLongArray>, Stream<TraversalStreamResult>> {

    private static final String RELATIONSHIP_TYPE = "NEXT";
    private final Graph graph;
    private final PathFactoryFacade pathFactoryFacade;
    private final long sourceNodeId;

    public TraversalStreamResultTransformer(
        Graph graph,
        PathFactoryFacade pathFactoryFacade,
        long sourceNodeId
    ) {
        this.graph = graph;
        this.pathFactoryFacade = pathFactoryFacade;
        this.sourceNodeId = sourceNodeId;
    }


    @Override
    public Stream<TraversalStreamResult> apply(TimedAlgorithmResult<HugeLongArray> result) {
        var nodes = result.result();

        var nodesArray = nodes.toArray();
        var nodeList = Arrays.stream(nodesArray)
            .map(graph::toOriginalNodeId)
            .boxed()
            .toList();

        var  path = pathFactoryFacade.createPath(
            nodeList,
            RelationshipType.withName(RELATIONSHIP_TYPE)
        );

        return Stream.of(
            new TraversalStreamResult(
                sourceNodeId, // should be node[0]
                nodeList,
                path
            )
        );
    }
}
