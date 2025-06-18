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
package org.neo4j.gds.msbfs;

import org.neo4j.gds.collections.ha.HugeLongArray;

public class EmptySourceNodesSpec implements SourceNodesSpec{

    private final int localNodeCount;
    private final long nodeOffset;

    public EmptySourceNodesSpec(long nodeOffset, int localNodeCount) {
        this.localNodeCount = localNodeCount;
        this.nodeOffset = nodeOffset;
    }

    @Override
    public SourceNodes setUp(HugeLongArray visitSet, HugeLongArray seenSet, boolean allowStartNodeTraversal) {

        for (int i = 0; i < localNodeCount; ++i) {
            long currentNode = nodeOffset + i;

            if (!allowStartNodeTraversal) {
                seenSet.set(currentNode, 1L << i);
            }

            visitSet.or(currentNode, 1L << i);
        }

        return new SourceNodes(nodeOffset, localNodeCount);
    }

    @Override
    public long[] nodes() {
        var array = new long[localNodeCount];
        for (int i=0; i<localNodeCount;++i){
            long currentNode = nodeOffset + i;
            array[i] = currentNode;
        }
        return array;
    }

}
