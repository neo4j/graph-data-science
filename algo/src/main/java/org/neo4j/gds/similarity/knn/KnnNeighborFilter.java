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
package org.neo4j.gds.similarity.knn;

public class KnnNeighborFilter implements NeighborFilter {
    private final long nodeCount;

    public KnnNeighborFilter(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public boolean excludeNodePair(long firstNodeId, long secondNodeId) {
        return firstNodeId == secondNodeId;
    }

    @Override
    public long lowerBoundOfPotentialNeighbours(long node) {
        // excluding the node itself
        return nodeCount - 1;
    }

    @Override
    public boolean isSymmetric() {
        return true;
    }
}
