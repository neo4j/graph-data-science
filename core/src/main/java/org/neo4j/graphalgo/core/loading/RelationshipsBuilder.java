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


import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyList;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

public class RelationshipsBuilder {

    private static final AdjacencyListBuilder[] EMPTY_PROPERTY_BUILDERS = new AdjacencyListBuilder[0];

    private final RelationshipProjection projection;
    final AdjacencyListBuilder adjacencyListBuilder;
    final AdjacencyListBuilder[] propertyBuilders;

    TransientAdjacencyOffsets globalAdjacencyOffsets;
    TransientAdjacencyOffsets[] globalPropertyOffsets;

    public RelationshipsBuilder(
        RelationshipProjection projection,
        AllocationTracker tracker
    ) {
        this.projection = projection;

        adjacencyListBuilder = AdjacencyListBuilder.newBuilder(tracker);

        if (projection.properties().isEmpty()) {
            propertyBuilders = EMPTY_PROPERTY_BUILDERS;
        } else {
            propertyBuilders = new AdjacencyListBuilder[projection.properties().numberOfMappings()];
            Arrays.setAll(propertyBuilders, i -> AdjacencyListBuilder.newBuilder(tracker));
        }
    }

    final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder(
            long[] adjacencyOffsets,
            long[][] propertyOffsets,
            Aggregation[] aggregations
    ) {
        return new ThreadLocalRelationshipsBuilder(
            adjacencyListBuilder.newAllocator(),
            Arrays.stream(propertyBuilders)
                .map(AdjacencyListBuilder::newAllocator)
                .toArray(AdjacencyListBuilder.Allocator[]::new),
            adjacencyOffsets,
            propertyOffsets,
            aggregations
        );
    }

    final void setGlobalAdjacencyOffsets(TransientAdjacencyOffsets globalAdjacencyOffsets) {
        this.globalAdjacencyOffsets = globalAdjacencyOffsets;
    }

    final void setGlobalPropertyOffsets(TransientAdjacencyOffsets[] globalPropertyOffsets) {
        this.globalPropertyOffsets = globalPropertyOffsets;
    }

    public TransientAdjacencyList adjacencyList() {
        return adjacencyListBuilder.build();
    }

    public TransientAdjacencyOffsets globalAdjacencyOffsets() {
        return globalAdjacencyOffsets;
    }

    // TODO: This returns only the first of possibly multiple properties
    public TransientAdjacencyList properties() {
        return propertyBuilders.length > 0 ? propertyBuilders[0].build() : null;
    }

    public TransientAdjacencyList properties(int propertyIndex) {
        return propertyBuilders.length > 0 ? propertyBuilders[propertyIndex].build() : null;
    }

    public RelationshipProjection projection() {
        return this.projection;
    }

    // TODO: This returns only the first of possibly multiple properties
    public TransientAdjacencyOffsets globalPropertyOffsets() {
        return globalPropertyOffsets[0];
    }

    public TransientAdjacencyOffsets globalPropertyOffsets(int propertyIndex) {
        return globalPropertyOffsets[propertyIndex];
    }
}
