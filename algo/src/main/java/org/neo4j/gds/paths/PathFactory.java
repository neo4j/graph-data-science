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
package org.neo4j.gds.paths;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public final class PathFactory {

    public static final long DEFAULT_RELATIONSHIP_OFFSET = -1L;

    private PathFactory() {}

    public static Path create(
        Transaction tx,
        long relationshipIdOffset,
        long[] nodeIds,
        double[] costs,
        RelationshipType relationshipType,
        String costPropertyName
    ) {
        var firstNodeId = nodeIds[0];
        var pathBuilder = new PathImpl.Builder(tx.getNodeById(firstNodeId));

        for (int i = 0; i < nodeIds.length - 1; i++) {
            long sourceNodeId = nodeIds[i];
            long targetNodeId = nodeIds[i + 1];

            var relationship = new VirtualRelationship(
                relationshipIdOffset--,
                tx.getNodeById(sourceNodeId),
                tx.getNodeById(targetNodeId),
                relationshipType
            );
            var costDifference = costs[i + 1] - costs[i];
            relationship.setProperty(costPropertyName, costDifference);
            pathBuilder = pathBuilder.push(relationship);
        }

        return pathBuilder.build();
    }
}
