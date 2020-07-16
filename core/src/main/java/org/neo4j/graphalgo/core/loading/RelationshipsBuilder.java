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
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.core.Aggregation;

import java.util.Arrays;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class RelationshipsBuilder {

    private static final AdjacencyListBuilder[] EMPTY_PROPERTY_BUILDERS = new AdjacencyListBuilder[0];

    private final RelationshipProjection projection;
    private final AdjacencyListBuilder adjacencyListBuilder;
    private final AdjacencyOffsetsFactory offsetsFactory;
    private final AdjacencyListBuilder[] propertyBuilders;
    private long[][] globalAdjacencyOffsetsPages;
    private AdjacencyOffsets globalAdjacencyOffsets;
    private long[][][] globalPropertyOffsetsPages;
    private AdjacencyOffsets[] globalPropertyOffsets;

    public RelationshipsBuilder(
        RelationshipProjection projection,
        AdjacencyListBuilderFactory listBuilderFactory,
        AdjacencyOffsetsFactory offsetsFactory
    ) {
        this.projection = projection;
        this.adjacencyListBuilder = listBuilderFactory.newAdjacencyListBuilder();
        this.offsetsFactory = offsetsFactory;

        if (projection.properties().isEmpty()) {
            this.propertyBuilders = EMPTY_PROPERTY_BUILDERS;
        } else {
            this.propertyBuilders = new AdjacencyListBuilder[projection.properties().numberOfMappings()];
            Arrays.setAll(propertyBuilders, i -> listBuilderFactory.newAdjacencyListBuilder());
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
                .toArray(AdjacencyListAllocator[]::new),
            adjacencyOffsets,
            propertyOffsets,
            aggregations
        );
    }

    final void setGlobalAdjacencyOffsets(long[][] pages) {
        this.globalAdjacencyOffsetsPages = pages;
    }

    final void setGlobalPropertyOffsets(long[][][] allPages) {
        globalPropertyOffsetsPages = allPages;
    }

    private boolean containsNonNull(long[][] pages) {
        return Arrays.stream(pages).noneMatch(Predicate.isEqual(null));
    }

    boolean supportsProperties() {
        // TODO temporary until Geri does support properties
        return adjacencyListBuilder instanceof TransientAdjacencyListBuilder;
    }

    public AdjacencyList adjacencyList() {
        return adjacencyListBuilder.build();
    }

    public AdjacencyOffsets globalAdjacencyOffsets() {
        if (globalAdjacencyOffsets == null) {
            globalAdjacencyOffsets = offsetsFactory.newOffsets(requireNonNull(globalAdjacencyOffsetsPages));
        }
        return globalAdjacencyOffsets;
    }

    // TODO: This returns only the first of possibly multiple properties
    public AdjacencyList properties() {
        return propertyBuilders.length > 0 ? propertyBuilders[0].build() : null;
    }

    public AdjacencyList properties(int propertyIndex) {
        return propertyBuilders.length > 0 ? propertyBuilders[propertyIndex].build() : null;
    }

    public RelationshipProjection projection() {
        return this.projection;
    }

    // TODO: This returns only the first of possibly multiple properties
    AdjacencyOffsets globalPropertyOffsets() {
        return globalPropertyOffsets(0);
    }

    public AdjacencyOffsets globalPropertyOffsets(int propertyIndex) {
        if (globalPropertyOffsets == null) {
            globalPropertyOffsets = new AdjacencyOffsets[requireNonNull(globalPropertyOffsetsPages).length];
            Arrays.setAll(globalPropertyOffsets, i -> {
                var pages = globalPropertyOffsetsPages[i];
                // TODO: the nested null check is for graphs that don't support properties (i.e. Geri)
                return pages != null && containsNonNull(pages) ? offsetsFactory.newOffsets(pages) : null;
            });
        }
        return globalPropertyOffsets[propertyIndex];
    }

    void flush() {
        adjacencyListBuilder.flush();
        for (AdjacencyListBuilder propertyBuilder : propertyBuilders) {
            if (propertyBuilder != null) {
                propertyBuilder.flush();
            }
        }
    }
}
