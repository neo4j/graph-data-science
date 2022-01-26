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

import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.core.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public final class AdjacencyListWithPropertiesBuilder {

    private final RelationshipProjection projection;
    private final AdjacencyCompressorFactory adjacencyCompressor;

    private final LongAdder relationshipCounter;
    private final Aggregation[] aggregations;
    private final int[] propertyKeyIds;
    private final double[] defaultValues;

    public static AdjacencyListWithPropertiesBuilder create(
        LongSupplier nodeCountSupplier,
        AdjacencyListBehavior adjacencyListBehavior,
        RelationshipProjection projection,
        Map<String, Integer> relationshipPropertyTokens,
        AllocationTracker allocationTracker
    ) {
        var aggregations = aggregations(projection);
        var propertyKeyIds = propertyKeyIds(projection, relationshipPropertyTokens);
        double[] defaultValues = defaultValues(projection);

        return create(
            nodeCountSupplier,
            adjacencyListBehavior,
            projection,
            aggregations,
            propertyKeyIds,
            defaultValues,
            allocationTracker
        );
    }

    public static AdjacencyListWithPropertiesBuilder create(
        LongSupplier nodeCountSupplier,
        AdjacencyListBehavior adjacencyListBehavior,
        RelationshipProjection projection,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        AllocationTracker allocationTracker
    ) {
        var relationshipCounter = new LongAdder();

        var adjacencyCompressorFactory = adjacencyListBehavior.create(
            nodeCountSupplier,
            projection.properties(),
            aggregations,
            allocationTracker
        );

        return new AdjacencyListWithPropertiesBuilder(
            projection,
            adjacencyCompressorFactory,
            relationshipCounter,
            aggregations,
            propertyKeyIds,
            defaultValues
        );
    }

    private static double[] defaultValues(RelationshipProjection projection) {
        return projection
            .properties()
            .mappings()
            .stream()
            .mapToDouble(propertyMapping -> propertyMapping.defaultValue().doubleValue())
            .toArray();
    }

    private static int[] propertyKeyIds(
        RelationshipProjection projection,
        Map<String, Integer> relationshipPropertyTokens
    ) {
        return projection.properties().mappings()
            .stream()
            .mapToInt(mapping -> relationshipPropertyTokens.get(mapping.neoPropertyKey())).toArray();
    }

    private static Aggregation[] aggregations(RelationshipProjection projection) {
        var propertyMappings = projection.properties().mappings();

        Aggregation[] aggregations = propertyMappings.stream()
            .map(PropertyMapping::aggregation)
            .map(Aggregation::resolve)
            .toArray(Aggregation[]::new);

        if (propertyMappings.isEmpty()) {
            aggregations = new Aggregation[]{Aggregation.resolve(projection.aggregation())};
        }

        return aggregations;
    }

    private AdjacencyListWithPropertiesBuilder(
        RelationshipProjection projection,
        AdjacencyCompressorFactory adjacencyCompressor,
        LongAdder relationshipCounter,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues
    ) {
        this.projection = projection;
        this.adjacencyCompressor = adjacencyCompressor;
        this.relationshipCounter = relationshipCounter;
        this.aggregations = aggregations;
        this.propertyKeyIds = propertyKeyIds;
        this.defaultValues = defaultValues;
    }

    ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder() {
        return new ThreadLocalRelationshipsBuilder(adjacencyCompressor);
    }

    void prepareAdjacencyListBuilderTasks() {
        this.adjacencyCompressor.init();
    }

    LongAdder relationshipCounter() {
        return relationshipCounter;
    }

    public AdjacencyListsWithProperties build() {
        return adjacencyCompressor.build()
            .relationshipCount(relationshipCounter.longValue())
            .build();
    }

    public RelationshipProjection projection() {
        return this.projection;
    }

    Aggregation[] aggregations() {
        return aggregations;
    }

    int[] propertyKeyIds() {
        return propertyKeyIds;
    }

    double[] defaultValues() {
        return defaultValues;
    }
}
