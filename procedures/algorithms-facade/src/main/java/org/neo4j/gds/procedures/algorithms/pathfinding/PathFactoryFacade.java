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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;

public class PathFactoryFacade {
    private final boolean canCreatePaths;
    private final NodeLookup nodeLookup;

   private PathFactoryFacade(boolean canCreatePaths, NodeLookup nodeLookup) {
        this.canCreatePaths = canCreatePaths;
        this.nodeLookup = nodeLookup;
    }

    public static PathFactoryFacade create(boolean pathIsYielded, NodeLookup nodeLookup, GraphStore graphStore){
        var canCreatePaths = pathIsYielded && graphStore.capabilities().canWriteToLocalDatabase();
        return new PathFactoryFacade(canCreatePaths,nodeLookup);
    }

    public Path createPath(
        List<Long> nodeList,
        RelationshipType relationshipType
    ) {
        if (!canCreatePaths) return  null;
        return PathFactory.create(
            nodeLookup,
            nodeList,
            relationshipType
        );
    }

    public Path createPath(
        long[] nodeList,
        double[] costs,
        RelationshipType relationshipType,
        String costPropertyName
    ) {
        if (!canCreatePaths) return  null;
        return PathFactory.create(
            nodeLookup,
            nodeList,
            costs,
            relationshipType,
            costPropertyName
        );
    }
}
