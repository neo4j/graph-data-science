/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.nn;

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphdb.Direction;
import org.roaringbitmap.RoaringBitmap;

public class NewOldGraph {
    private final HugeGraph graph;
    private final RoaringBitmap[] visitedRelationships;

    NewOldGraph(HugeGraph graph, RoaringBitmap[] visitedRelationships) {
        this.graph = graph;
        this.visitedRelationships = visitedRelationships;
    }

    LongHashSet findOldNeighbors(final long nodeId) {
        LongHashSet neighbors = new LongHashSet();
        RoaringBitmap visited = visitedRelationships[(int) nodeId];

        graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
            if (visited.contains((int) targetNodeId)) {
                neighbors.add(targetNodeId);
            }

            return true;
        });
        return neighbors;
    }


    LongHashSet findNewNeighbors(final long nodeId) {
        LongHashSet neighbors = new LongHashSet();

        RoaringBitmap visited = visitedRelationships[(int) nodeId];

        graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
            if (!visited.contains((int) targetNodeId)) {
                neighbors.add(targetNodeId);
            }

            return true;
        });
        return neighbors;
    }
}
