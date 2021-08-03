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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compress.AdjacencyCompressorBlueprint;
import org.neo4j.gds.core.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.compress.ImmutableAdjacencyListsWithProperties;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

abstract class AbstractCompressorBlueprint<TARGET_PAGE, PROPERTY_PAGE> implements AdjacencyCompressorBlueprint {

    final CsrListBuilder<TARGET_PAGE, ? extends AdjacencyList> adjacencyBuilder;
    final CsrListBuilder<PROPERTY_PAGE, ? extends AdjacencyProperties>[] propertyBuilders;
    final HugeIntArray adjacencyDegrees;
    final HugeLongArray adjacencyOffsets;
    final HugeLongArray[] propertyOffsets;
    final boolean noAggregation;
    final Aggregation[] aggregations;

    AbstractCompressorBlueprint(
        CsrListBuilder<TARGET_PAGE, ? extends AdjacencyList> adjacencyBuilder,
        CsrListBuilder<PROPERTY_PAGE, ? extends AdjacencyProperties>[] propertyBuilders,
        HugeIntArray adjacencyDegrees,
        HugeLongArray adjacencyOffsets,
        HugeLongArray[] propertyOffsets,
        boolean noAggregation,
        Aggregation[] aggregations
    ) {
        this.adjacencyBuilder = adjacencyBuilder;
        this.propertyBuilders = propertyBuilders;
        this.adjacencyDegrees = adjacencyDegrees;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.noAggregation = noAggregation;
        this.aggregations = aggregations;
    }

    @Override
    public void flush() {
        adjacencyBuilder.flush();
        for (var propertyBuilder : propertyBuilders) {
            if (propertyBuilder != null) {
                propertyBuilder.flush();
            }
        }
    }

    @Override
    public AdjacencyListsWithProperties build() {
        var builder = ImmutableAdjacencyListsWithProperties
            .builder()
            .adjacency(adjacencyBuilder.build(this.adjacencyDegrees, this.adjacencyOffsets));

        for (int i = 0; i < propertyBuilders.length; i++) {
            var properties = propertyBuilders[i].build(this.adjacencyDegrees, propertyOffsets[i]);
            builder.addProperty(properties);
        }

        return builder.build();
    }

}
