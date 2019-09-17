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

import org.neo4j.graphalgo.core.DeduplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

class RelationshipsBuilder {

    private final DeduplicateRelationshipsStrategy deduplicateRelationshipsStrategy;
    private final boolean weighted;
    final AdjacencyListBuilder adjacency;
    final AdjacencyListBuilder weights;

    AdjacencyOffsets globalAdjacencyOffsets;
    AdjacencyOffsets globalWeightOffsets;

    RelationshipsBuilder(
            DeduplicateRelationshipsStrategy deduplicateRelationshipsStrategy,
            AllocationTracker tracker,
            boolean weighted) {
        if (deduplicateRelationshipsStrategy == DeduplicateRelationshipsStrategy.DEFAULT) {
            throw new IllegalArgumentException("Needs an explicit deduplicateRelationshipsStrategy, but got " + deduplicateRelationshipsStrategy);
        }
        this.deduplicateRelationshipsStrategy = deduplicateRelationshipsStrategy;
        this.weighted = weighted;
        adjacency = AdjacencyListBuilder.newBuilder(tracker);
        weights = weighted ? AdjacencyListBuilder.newBuilder(tracker) : null;
    }

    final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder(
            long[] adjacencyOffsets,
            long[] weightOffsets) {
        return new ThreadLocalRelationshipsBuilder(
                deduplicateRelationshipsStrategy,
                adjacency.newAllocator(),
                weighted ? weights.newAllocator() : null,
                adjacencyOffsets,
                weightOffsets);
    }

    final void setGlobalAdjacencyOffsets(AdjacencyOffsets globalAdjacencyOffsets) {
        this.globalAdjacencyOffsets = globalAdjacencyOffsets;
    }

    final void setGlobalWeightOffsets(AdjacencyOffsets globalWeightOffsets) {
        this.globalWeightOffsets = globalWeightOffsets;
    }
}
