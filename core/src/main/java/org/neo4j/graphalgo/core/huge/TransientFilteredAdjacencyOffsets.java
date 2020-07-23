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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.core.loading.IdMap;

public class TransientFilteredAdjacencyOffsets implements AdjacencyOffsets {

    private final IdMap filteredIdMap;
    private final AdjacencyOffsets transientAdjacencyOffsets;

    public TransientFilteredAdjacencyOffsets(IdMap filteredIdMap, AdjacencyOffsets transientAdjacencyOffsets) {
        this.filteredIdMap = filteredIdMap;
        this.transientAdjacencyOffsets = transientAdjacencyOffsets;
    }

    @Override
    public long get(long index) {
        if (filteredIdMap.contains(index)) {
            return transientAdjacencyOffsets.get(filteredIdMap.toOriginalNodeId(index));
        }
        return 0;
    }

    @Override
    public void close() {
        transientAdjacencyOffsets.close();
    }
}
