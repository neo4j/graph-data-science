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
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressor;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorFactory;
import org.neo4j.graphalgo.core.compress.AdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Arrays;
import java.util.Map;

public final class RelationshipsBuilder {

    private static final AdjacencyListBuilder[] EMPTY_PROPERTY_BUILDERS = new AdjacencyListBuilder[0];

    private final RelationshipProjection projection;

    private final AdjacencyListBuilder adjacencyListBuilder;
    private final AdjacencyCompressor adjacencyCompressor;

    private final AdjacencyListBuilder[] propertyBuilders;
    private final Aggregation[] aggregations;
    private final int[] propertyKeyIds;
    private final double[] defaultValues;

    public static RelationshipsBuilder create(
        long nodeCount,
        RelationshipProjection projection,
        Map<String, Integer> relationshipPropertyTokens,
        AdjacencyListBuilderFactory listBuilderFactory,
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
            offsetsFactory,
            aggregations,
            propertyKeyIds,
            defaultValues,
            tracker
        );
    }

    public static RelationshipsBuilder create(
        long nodeCount,
        RelationshipProjection projection,
        AdjacencyListBuilderFactory listBuilderFactory,
        AdjacencyOffsetsFactory offsetsFactory,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        AllocationTracker tracker
    ) {
        return new RelationshipsBuilder(
            nodeCount,
            projection,
            listBuilderFactory,
            offsetsFactory,
            aggregations,
            propertyKeyIds,
            defaultValues,
            tracker
        );
    }

    // TODO
    static AdjacencyCompressorFactory adjacencyCompressorFactory(AdjacencyOffsetsFactory offsetsFactory) {
        return new DeltaVarLongCompressor.Factory(offsetsFactory);
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

    private RelationshipsBuilder(
        long nodeCount,
        RelationshipProjection projection,
        AdjacencyListBuilderFactory listBuilderFactory,
        AdjacencyOffsetsFactory offsetsFactory,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        AllocationTracker tracker
    ) {
        this(
            nodeCount,
            projection,
            adjacencyCompressorFactory(offsetsFactory),
            listBuilderFactory,
            aggregations,
            propertyKeyIds,
            defaultValues,
            tracker
        );
    }

    private RelationshipsBuilder(
        long nodeCount,
        RelationshipProjection projection,
        AdjacencyCompressorFactory adjacencyCompressorFactory,
        AdjacencyListBuilderFactory listBuilderFactory,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        AllocationTracker tracker
    ) {
        this.projection = projection;
        this.adjacencyListBuilder = listBuilderFactory.newAdjacencyListBuilder();
        this.aggregations = aggregations;
        this.propertyKeyIds = propertyKeyIds;
        this.defaultValues = defaultValues;

        if (projection.properties().isEmpty()) {
            this.propertyBuilders = EMPTY_PROPERTY_BUILDERS;
        } else {
            this.propertyBuilders = new AdjacencyListBuilder[projection.properties().numberOfMappings()];
            Arrays.setAll(propertyBuilders, i -> listBuilderFactory.newAdjacencyListBuilder());
        }

        // TODO move to factory methods?
        adjacencyCompressor = adjacencyCompressorFactory.create(
            nodeCount,
            adjacencyListBuilder,
            propertyBuilders,
            aggregations,
            tracker
        );
    }

    ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder() {
        return new ThreadLocalRelationshipsBuilder(adjacencyCompressor.concurrentCopy());
    }

    boolean supportsProperties() {
        // TODO temporary until Geri does support properties
        return adjacencyListBuilder instanceof TransientAdjacencyListBuilder;
    }

    public AdjacencyListsWithProperties build() {
        return adjacencyCompressor.build();
    }

    public AdjacencyList adjacencyList() {
        return adjacencyListBuilder.build();
    }

    // TODO: This returns only the first of possibly multiple properties
    public AdjacencyList properties() {
        return propertyBuilders.length > 0 ? propertyBuilders[0].build() : null;
    }

    public AdjacencyList properties(int propertyIndex) {
        return propertyBuilders.length > 0 ? propertyBuilders[propertyIndex].build() : null;
    }

    // TODO: maybe remove
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
        adjacencyListBuilder.flush();
        for (AdjacencyListBuilder propertyBuilder : propertyBuilders) {
            if (propertyBuilder != null) {
                propertyBuilder.flush();
            }
        }
    }
}
