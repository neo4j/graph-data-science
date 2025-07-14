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
package org.neo4j.gds.cliqueCounting.intersect;

import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.huge.NodeFilteredAdjacencyCursor;
import org.neo4j.gds.core.huge.NodeFilteredGraph;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.gds.api.properties.relationships.RelationshipIterator}s.
 */

public final class NodeFilteredCliqueIntersect implements CliqueAdjacency {

    private final NodeFilteredGraph nodeFilteredGraph;
    private final CliqueAdjacency innerCliqueAdjacency;

    public NodeFilteredCliqueIntersect(NodeFilteredGraph nodeFilteredGraph, CliqueAdjacency innerCliqueAdjacency) {
        this.nodeFilteredGraph = nodeFilteredGraph;
        this.innerCliqueAdjacency = innerCliqueAdjacency;
    }

    @Override
    public AdjacencyCursor createCursor(long node) {
        if (nodeFilteredGraph.containsRootNodeId(node)) {
            return new NodeFilteredAdjacencyCursor(innerCliqueAdjacency.createCursor(node),nodeFilteredGraph );
        } else{
            return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
        }
    }


}
