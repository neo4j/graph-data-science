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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

class RelationshipsBuilder {

    private final DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;
    private final boolean weighted;
    final HugeAdjacencyListBuilder adjacency;
    final HugeAdjacencyListBuilder weights;

    HugeAdjacencyOffsets globalAdjacencyOffsets;
    HugeAdjacencyOffsets globalWeightOffsets;

    RelationshipsBuilder(
            DuplicateRelationshipsStrategy duplicateRelationshipsStrategy,
            AllocationTracker tracker,
            boolean weighted) {
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy == DuplicateRelationshipsStrategy.DEFAULT ? DuplicateRelationshipsStrategy.SKIP : duplicateRelationshipsStrategy;
        this.weighted = weighted;
        adjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
        weights = weighted ? HugeAdjacencyListBuilder.newBuilder(tracker) : null;
    }

    final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder(
            long[] adjacencyOffsets,
            long[] weightOffsets) {
        return new ThreadLocalRelationshipsBuilder(
                duplicateRelationshipsStrategy,
                adjacency.newAllocator(),
                weighted ? weights.newAllocator() : null,
                adjacencyOffsets,
                weightOffsets);
    }

    final void setGlobalAdjacencyOffsets(HugeAdjacencyOffsets globalAdjacencyOffsets) {
        this.globalAdjacencyOffsets = globalAdjacencyOffsets;
    }

    final void setGlobalWeightOffsets(HugeAdjacencyOffsets globalWeightOffsets) {
        this.globalWeightOffsets = globalWeightOffsets;
    }
}
