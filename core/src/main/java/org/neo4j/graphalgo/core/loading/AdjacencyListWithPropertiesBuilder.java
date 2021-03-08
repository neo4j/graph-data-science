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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorBlueprint;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorFactory;
import org.neo4j.graphalgo.core.compress.AdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Arrays;
import java.util.Map;

public final class AdjacencyListWithPropertiesBuilder {

    private static final AdjacencyListBuilder[] EMPTY_PROPERTY_BUILDERS = new AdjacencyListBuilder[0];

    private final RelationshipProjection projection;
    private final AdjacencyCompressorBlueprint adjacencyCompressor;

    private final Aggregation[] aggregations;
    private final int[] propertyKeyIds;
    private final double[] defaultValues;

    public static AdjacencyListWithPropertiesBuilder create(
        long nodeCount,
        RelationshipProjection projection,
        Map<String, Integer> relationshipPropertyTokens,
        AdjacencyListBuilderFactory listBuilderFactory,
        AdjacencyDegreesFactory degreesFactory,
        AdjacencyOffsetsFactory offsetsFactory,
        AllocationTracker tracker
    ) {
        var aggregations = aggregations(projection);
        var propertyKeyIds = propertyKeyIds(projection, relationshipPropertyTokens);
        double[] defaultValues = defaultValues(projection);

        return create(
            nodeCount,
            projection,
            listBuilderFactory,
            degreesFactory,
            offsetsFactory,
            aggregations,
            propertyKeyIds,
            defaultValues,
            tracker
        );
    }

    public static AdjacencyListWithPropertiesBuilder create(
        long nodeCount,
        RelationshipProjection projection,
        AdjacencyListBuilderFactory listBuilderFactory,
        AdjacencyDegreesFactory degreesFactory,
        AdjacencyOffsetsFactory offsetsFactory,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        AllocationTracker tracker
    ) {
        return create(
            nodeCount,
            projection,
            adjacencyCompressorFactory(degreesFactory, offsetsFactory),
            listBuilderFactory,
            aggregations,
            propertyKeyIds,
            defaultValues,
            tracker
        );
    }

    private static AdjacencyListWithPropertiesBuilder create(
        long nodeCount,
        RelationshipProjection projection,
        AdjacencyCompressorFactory adjacencyCompressorFactory,
        AdjacencyListBuilderFactory listBuilderFactory,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        AllocationTracker tracker
    ) {
        var adjacencyListBuilder = listBuilderFactory.newAdjacencyListBuilder();

        var propertyBuilders = EMPTY_PROPERTY_BUILDERS;
        if (!projection.properties().isEmpty()) {
            propertyBuilders = new AdjacencyListBuilder[projection.properties().numberOfMappings()];
            Arrays.setAll(propertyBuilders, i -> listBuilderFactory.newAdjacencyListBuilder());
        }

        var adjacencyCompressor = adjacencyCompressorFactory.create(
            nodeCount,
            adjacencyListBuilder,
            propertyBuilders,
            aggregations,
            tracker
        );

        return new AdjacencyListWithPropertiesBuilder(
            projection,
            adjacencyCompressor,
            aggregations,
            propertyKeyIds,
            defaultValues
        );
    }

    private static AdjacencyCompressorFactory adjacencyCompressorFactory(
        AdjacencyDegreesFactory degreesFactory,
        AdjacencyOffsetsFactory offsetsFactory
    ) {
        return new DeltaVarLongCompressor.Factory(degreesFactory, offsetsFactory);
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
        AdjacencyCompressorBlueprint adjacencyCompressor,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues
    ) {
        this.projection = projection;
        this.adjacencyCompressor = adjacencyCompressor;
        this.aggregations = aggregations;
        this.propertyKeyIds = propertyKeyIds;
        this.defaultValues = defaultValues;
    }

    ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder() {
        return new ThreadLocalRelationshipsBuilder(adjacencyCompressor.createCompressor());
    }

    boolean supportsProperties() {
        return adjacencyCompressor.supportsProperties();
    }

    public AdjacencyListsWithProperties build() {
        return adjacencyCompressor.build();
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

    void flush() {
        adjacencyCompressor.flush();
    }
}
