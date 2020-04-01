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


import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

public class RelationshipsBuilder {

    private static final AdjacencyListBuilder[] EMPTY_WEIGHTS = new AdjacencyListBuilder[0];

    private final Aggregation[] aggregations;
    final AdjacencyListBuilder adjacencyListBuilder;
    final AdjacencyListBuilder[] propertyBuilders;

    AdjacencyOffsets globalAdjacencyOffsets;
    AdjacencyOffsets[] globalPropertyOffsets;

    public RelationshipsBuilder(
        Aggregation[] aggregations,
        AllocationTracker tracker,
        int numberOfRelationshipProperties
    ) {
        if (Arrays.stream(aggregations).anyMatch(d -> d == Aggregation.DEFAULT)) {
            throw new IllegalArgumentException(String.format(
                "Needs an explicit aggregation, but got %s",
                Arrays.toString(aggregations)
            ));
        }
        this.aggregations = aggregations;
        adjacencyListBuilder = AdjacencyListBuilder.newBuilder(tracker);
        if (numberOfRelationshipProperties > 0) {
            propertyBuilders = new AdjacencyListBuilder[numberOfRelationshipProperties];
            Arrays.setAll(propertyBuilders, i -> AdjacencyListBuilder.newBuilder(tracker));
        } else {
            propertyBuilders = EMPTY_WEIGHTS;
        }
    }

    final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder(
            long[] adjacencyOffsets,
            long[][] weightOffsets
    ) {
        return new ThreadLocalRelationshipsBuilder(
            aggregations,
            adjacencyListBuilder.newAllocator(),
            Arrays.stream(propertyBuilders)
                .map(AdjacencyListBuilder::newAllocator)
                .toArray(AdjacencyListBuilder.Allocator[]::new),
            adjacencyOffsets,
            weightOffsets
        );
    }

    final void setGlobalAdjacencyOffsets(AdjacencyOffsets globalAdjacencyOffsets) {
        this.globalAdjacencyOffsets = globalAdjacencyOffsets;
    }

    final void setGlobalPropertyOffsets(AdjacencyOffsets[] globalPropertyOffsets) {
        this.globalPropertyOffsets = globalPropertyOffsets;
    }

    public AdjacencyList adjacencyList() {
        return adjacencyListBuilder.build();
    }

    public AdjacencyOffsets globalAdjacencyOffsets() {
        return globalAdjacencyOffsets;
    }

    // TODO: This returns only the first of possibly multiple properties
    public AdjacencyList properties() {
        return propertyBuilders.length > 0 ? propertyBuilders[0].build() : null;
    }

    public AdjacencyList properties(int propertyIndex) {
        return propertyBuilders.length > 0 ? propertyBuilders[propertyIndex].build() : null;
    }

    // TODO: This returns only the first of possibly multiple properties
    public AdjacencyOffsets globalPropertyOffsets() {
        return globalPropertyOffsets[0];
    }

    public AdjacencyOffsets globalPropertyOffsets(int propertyIndex) {
        return globalPropertyOffsets[propertyIndex];
    }
}
