/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Map;

public class FilteredIdMap extends IdMap {

    private final HugeLongArray newGraphIds;

    public FilteredIdMap(
        HugeLongArray graphIds,
        HugeLongArray newGraphIds,
        SparseNodeMapping newNodeToGraphIds,
        Map<String, BitSet> labelInformation,
        long nodeCount
    ) {
        super(graphIds, newNodeToGraphIds, labelInformation, nodeCount);
        this.newGraphIds = newGraphIds;
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return nodeToGraphIds.get(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return graphIds.get(newGraphIds.get(nodeId));
    }

    @Override
    public boolean contains(long nodeId) {
        return nodeToGraphIds.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }
}
