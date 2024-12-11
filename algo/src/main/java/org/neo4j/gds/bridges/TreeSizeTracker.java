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
package org.neo4j.gds.bridges;

import org.neo4j.gds.collections.ha.HugeLongArray;

 class TreeSizeTracker  {

    private final HugeLongArray subTreeSize;

    TreeSizeTracker(long nodeCount){
        subTreeSize = HugeLongArray.newArray(nodeCount);
        subTreeSize.setAll( v -> 1);
    }
    void recordTreeChild(long parent, long child) {
        subTreeSize.addTo(parent,subTreeSize.get(child));
    }

    public long[] recordBridge( long child, long root) {
        var childSubTree = subTreeSize.get(child);
        var rootSubTree= subTreeSize.get(root) - childSubTree;
        return  new long[]{rootSubTree, childSubTree};
    }

}
