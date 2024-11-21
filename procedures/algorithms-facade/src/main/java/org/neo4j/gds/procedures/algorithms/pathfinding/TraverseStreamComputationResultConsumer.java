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

import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class TraverseStreamComputationResultConsumer {
    private TraverseStreamComputationResultConsumer() {}

    public static <T> Stream<T> consume(
        long sourceNodeId,
        HugeLongArray nodes,
        LongUnaryOperator toOriginalNodeId,
        ConcreteResultTransformer<T> resultTransformer,
        PathFactoryFacade pathFactoryFacade,
        RelationshipType relationshipType,
        NodeLookup nodeLookup
    ) {
        var nodesArray = nodes.toArray();
        var nodeList = Arrays
            .stream(nodesArray)
            .boxed()
            .map(toOriginalNodeId::applyAsLong)
            .collect(Collectors.toList());

          var  path = pathFactoryFacade.createPath(
                nodeList,
                relationshipType
            );

        return Stream.of(resultTransformer.transform(
            sourceNodeId,
            nodeList,
            path
        ));
    }
}
