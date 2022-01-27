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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.Aggregation;

import java.util.Map;

/**
 * Wraps a relationship buffer that is being filled by the store scanners.
 * Forwards the relationship buffer to the
 * {@link org.neo4j.gds.core.loading.RelationshipImporter}
 * which prepares the buffer content for consumption by the
 * {@link AdjacencyBuffer}.
 *
 * Each importing thread holds an instance of this class for each relationship
 * type that is being imported.
 */
public final class ThreadLocalSingleTypeRelationshipImporter {

    private final RelationshipImporter.Imports imports;
    private final RelationshipImporter.PropertyReader propertyReader;
    private final RelationshipsBatchBuffer relationshipsBatchBuffer;

    ThreadLocalSingleTypeRelationshipImporter(
        RelationshipImporter.Imports imports,
        RelationshipImporter.PropertyReader propertyReader,
        RelationshipsBatchBuffer relationshipsBatchBuffer
    ) {
        this.imports = imports;
        this.propertyReader = propertyReader;
        this.relationshipsBatchBuffer = relationshipsBatchBuffer;
    }

    public RelationshipsBatchBuffer buffer() {
        return relationshipsBatchBuffer;
    }

    public long importRelationships() {
        return imports.importRelationships(relationshipsBatchBuffer, propertyReader);
    }

    @ValueClass
    public interface ImportMetaData {
        RelationshipProjection projection();

        Aggregation[] aggregations();

        int[] propertyKeyIds();

        double[] defaultValues();

        int typeTokenId();

        boolean preAggregate();

        static ImportMetaData of(RelationshipProjection projection, int typeTokenId, Map<String, Integer> relationshipPropertyTokens, boolean preAggregate) {
            return ImmutableImportMetaData
                .builder()
                .projection(projection)
                .aggregations(aggregations(projection))
                .propertyKeyIds(propertyKeyIds(projection, relationshipPropertyTokens))
                .defaultValues(defaultValues(projection))
                .typeTokenId(typeTokenId)
                .preAggregate(preAggregate)
                .build();
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
    }

    @ValueClass
    public interface RelationshipTypeImportContext {
        RelationshipType relationshipType();

        RelationshipProjection relationshipProjection();

        SingleTypeRelationshipImporter singleTypeRelationshipImporter();
    }
}
