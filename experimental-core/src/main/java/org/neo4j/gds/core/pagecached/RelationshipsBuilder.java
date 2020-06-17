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
package org.neo4j.gds.core.pagecached;


import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.io.pagecache.PageCache;

import java.io.IOException;
import java.util.Arrays;

public class RelationshipsBuilder {

    private static final AdjacencyListBuilder[] EMPTY_PROPERTY_BUILDERS = new AdjacencyListBuilder[0];

    final AdjacencyListBuilder adjacencyListBuilder;
    final AdjacencyListBuilder[] propertyBuilders;

    long[][] globalAdjacencyOffsets;
    AdjacencyOffsets[] globalPropertyOffsets;

    public RelationshipsBuilder(
        PageCache pageCache,
        RelationshipProjection projection
    ) {
        adjacencyListBuilder = AdjacencyListBuilder.newBuilder(pageCache);

        if (projection.properties().isEmpty()) {
            propertyBuilders = EMPTY_PROPERTY_BUILDERS;
        } else {
            propertyBuilders = new AdjacencyListBuilder[projection.properties().numberOfMappings()];
            Arrays.setAll(propertyBuilders, i -> AdjacencyListBuilder.newBuilder(pageCache));
        }
    }

    final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder(
            long[] adjacencyOffsets
    ) {
        return new ThreadLocalRelationshipsBuilder(
            adjacencyListBuilder.newAllocator(),
            adjacencyOffsets
        );
    }

    final void setGlobalAdjacencyOffsets(long[][] globalAdjacencyOffsets) {
        this.globalAdjacencyOffsets = globalAdjacencyOffsets;
    }

    final void setGlobalPropertyOffsets(AdjacencyOffsets[] globalPropertyOffsets) {
        this.globalPropertyOffsets = globalPropertyOffsets;
    }

    public AdjacencyList adjacencyList() throws IOException {
        return adjacencyListBuilder.build();
    }

    public AdjacencyOffsets globalAdjacencyOffsets(PageCache pageCache) throws IOException {
        return AdjacencyOffsets.of(pageCache, globalAdjacencyOffsets);
    }
}
