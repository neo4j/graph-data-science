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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.collections.ha.HugeLongArray;

class ClusterHierarchyUnionFind {

    private static final long DEFAULT_PARENT_VALUE = -1L;

    private final HugeLongArray parents;
    private long nextLabel;

    public ClusterHierarchyUnionFind(long nodeCount) {
        var size = (2 * nodeCount) - 1;
        this.parents = HugeLongArray.newArray(size);
        this.parents.fill(DEFAULT_PARENT_VALUE);
        this.nextLabel = nodeCount;
    }

    long union(long x, long y) {
        parents.set(x, nextLabel);
        parents.set(y, nextLabel);
        return nextLabel++;
    }

    long find(long x) {
        while (parents.get(x) != DEFAULT_PARENT_VALUE) {
            var parent = parents.get(x);
            var grandParent = parents.get(parent);
            if (grandParent == DEFAULT_PARENT_VALUE) {
                return parent;
            }

            parents.set(x, grandParent);
            x = grandParent;
        }
        return x;
    }
}
