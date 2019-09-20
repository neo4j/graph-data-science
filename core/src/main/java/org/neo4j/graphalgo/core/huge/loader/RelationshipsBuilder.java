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


import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

public class RelationshipsBuilder {

    private static final AdjacencyListBuilder[] EMPTY_WEIGHTS = new AdjacencyListBuilder[0];

    private final DeduplicationStrategy[] deduplicationStrategies;
    final AdjacencyListBuilder adjacency;
    final AdjacencyListBuilder[] weights;

    AdjacencyOffsets globalAdjacencyOffsets;
    AdjacencyOffsets[] globalWeightOffsets;

    public RelationshipsBuilder(
            DeduplicationStrategy[] deduplicationStrategies,
            AllocationTracker tracker,
            int numberOfRelationshipWeights) {
        if (Arrays.stream(deduplicationStrategies).anyMatch(d -> d == DeduplicationStrategy.DEFAULT)) {
            throw new IllegalArgumentException(String.format(
                    "Needs an explicit deduplicateRelationshipsStrategy, but got %s",
                    Arrays.toString(deduplicationStrategies)
            ));
        }
        this.deduplicationStrategies = deduplicationStrategies;
        adjacency = AdjacencyListBuilder.newBuilder(tracker);
        if (numberOfRelationshipWeights > 0) {
            weights = new AdjacencyListBuilder[numberOfRelationshipWeights];
            // TODO: can we avoid to create an allocator/complete adjacency list
            //  if we know that the property does not exist?
            Arrays.setAll(weights, i -> AdjacencyListBuilder.newBuilder(tracker));
        } else {
            weights = EMPTY_WEIGHTS;
        }
    }

    final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder(
            long[] adjacencyOffsets,
            long[][] weightOffsets) {
        return new ThreadLocalRelationshipsBuilder(
                deduplicationStrategies,
                adjacency.newAllocator(),
                Arrays.stream(weights)
                        .map(AdjacencyListBuilder::newAllocator)
                        .toArray(AdjacencyListBuilder.Allocator[]::new),
                adjacencyOffsets,
                weightOffsets);
    }

    final void setGlobalAdjacencyOffsets(AdjacencyOffsets globalAdjacencyOffsets) {
        this.globalAdjacencyOffsets = globalAdjacencyOffsets;
    }

    final void setGlobalWeightOffsets(AdjacencyOffsets[] globalWeightOffsets) {
        this.globalWeightOffsets = globalWeightOffsets;
    }

    public AdjacencyList adjacency() {
        return adjacency.build();
    }

    public AdjacencyList weights() {
        return weights.length > 0 ? weights[0].build() : null;
    }

    public AdjacencyOffsets globalAdjacencyOffsets() {
        return globalAdjacencyOffsets;
    }

    public AdjacencyOffsets globalWeightOffsets() {
        return globalWeightOffsets[0];
    }
}
