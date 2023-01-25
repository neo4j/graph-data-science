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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

public final class PathFactory {

    private PathFactory() {}

    public static Path create(
        LongFunction<Node> nodeLookup,
        long[] nodeIds,
        double[] costs,
        RelationshipType relationshipType,
        String costPropertyName
    ) {
        var firstNodeId = nodeIds[0];
        var pathBuilder = new PathImpl.Builder(nodeLookup.apply(firstNodeId));

        for (int i = 0; i < nodeIds.length - 1; i++) {
            long sourceNodeId = nodeIds[i];
            long targetNodeId = nodeIds[i + 1];

            var relationship = Neo4jProxy.virtualRelationship(
                RelationshipIds.next(),
                nodeLookup.apply(sourceNodeId),
                nodeLookup.apply(targetNodeId),
                relationshipType
            );
            var costDifference = costs[i + 1] - costs[i];
            relationship.setProperty(costPropertyName, costDifference);
            pathBuilder = pathBuilder.push(relationship);
        }

        return pathBuilder.build();
    }

    public static Path create(
        LongFunction<Node> nodeLookup,
        List<Long> nodeIds,
        RelationshipType relationshipType
    ) {
        var firstNodeId = nodeIds.get(0);
        var pathBuilder = new PathImpl.Builder(nodeLookup.apply(firstNodeId));

        for (int i = 0; i < nodeIds.size() - 1; i++) {
            long sourceNodeId = nodeIds.get(i);
            long targetNodeId = nodeIds.get(i + 1);

            var relationship = Neo4jProxy.virtualRelationship(
                RelationshipIds.next(),
                nodeLookup.apply(sourceNodeId),
                nodeLookup.apply(targetNodeId),
                relationshipType
            );
            pathBuilder = pathBuilder.push(relationship);
        }

        return pathBuilder.build();
    }

    public static final class RelationshipIds {

        static final AtomicLong ids = new AtomicLong(0);

        static long next() {
            var nextId = ids.getAndDecrement();

            while (nextId > 0) {
                ids.compareAndSet(nextId - 1, 0);
                nextId = ids.getAndDecrement();
            }

            return nextId;
        }

        @TestOnly
        public static void set(long value) {
            ids.set(value);
        }

        private RelationshipIds() {}
    }
}
