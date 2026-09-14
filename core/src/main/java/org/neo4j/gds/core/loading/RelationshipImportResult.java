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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.GenerateBuilder;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.relationships.Properties;
import org.neo4j.gds.api.properties.relationships.RelationshipProperty;
import org.neo4j.gds.api.properties.relationships.RelationshipPropertyStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchemaEntry;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@GenerateBuilder
public record RelationshipImportResult(Map<RelationshipType, SingleTypeRelationships> importResults) {

    public static RelationshipImportResult empty() {
        return new RelationshipImportResult(Map.of());
    }

    public MutableRelationshipSchema relationshipSchema() {
        var relationshipSchema = MutableRelationshipSchema.empty();
        importResults().forEach((__, relationships) -> relationshipSchema.set(relationships.relationshipSchemaEntry()));
        return relationshipSchema;
    }

    /**
     * This method creates the final {@link RelationshipImportResult} in preparation
     * for the {@link org.neo4j.gds.api.GraphStore}.
     * <p>
     * The method is used in the context of native projection, where each projected relationship type (and its
     * properties) is represented by a {@link org.neo4j.gds.core.loading.SingleTypeRelationshipImporter.SingleTypeRelationshipImportContext}.
     *
     * @param importContexts each import context maps to a relationship type being created
     * @return a wrapper type ready to be consumed by a {@link org.neo4j.gds.api.GraphStore}
     */
    public static RelationshipImportResult of(Collection<SingleTypeRelationshipImporter.SingleTypeRelationshipImportContext> importContexts) {
        var buildersPerType = new HashMap<RelationshipType, SingleTypeRelationshipsBuilder>(importContexts.size());

        importContexts.forEach((importContext) -> {
            var adjacencyListsWithProperties = importContext.singleTypeRelationshipImporter().build();
            var isInverseRelationship = importContext.inverseOfRelationshipType().isPresent();

            var direction = Direction.fromOrientation(importContext.relationshipProjection().orientation());
            var topology = new Topology(
                adjacencyListsWithProperties.adjacency(),
                adjacencyListsWithProperties.relationshipCount(),
                importContext.relationshipProjection().isMultiGraph()
            );

            var schemaEntry = new MutableRelationshipSchemaEntry(importContext.relationshipType(), direction);
            RelationshipPropertyStore properties = null;
            if (!importContext.relationshipProjection().properties().isEmpty()) {
                properties = constructRelationshipPropertyStore(
                    importContext.relationshipProjection().properties().mappings(),
                    adjacencyListsWithProperties.properties(),
                    adjacencyListsWithProperties.relationshipCount()
                );
                properties.relationshipProperties().forEach((key, prop) -> schemaEntry.addProperty(key, prop.propertySchema()));
            }

            // With inverse relationships, we may see two import contexts for the same type.
            // These are to be combined in one RelationshipImportResult.
            // That is why we 1) use builders 2) use computeIfAbsent, so that we look up the same builder the second
            // time we see a type, and continue building the same result.
            var importResultBuilder = buildersPerType.computeIfAbsent(
                importContext.relationshipType(), __ -> SingleTypeRelationshipsBuilder.builder()
            );
            importResultBuilder.relationshipSchemaEntry(schemaEntry);
            if (isInverseRelationship) {
                importResultBuilder.inverseTopology(topology).inverseProperties(properties);
            } else {
                importResultBuilder.topology(topology).properties(properties);
            }
        });

        var relationshipImportResultBuilder = RelationshipImportResultBuilder.builder();
        buildersPerType.forEach((type, builderForType)
            -> relationshipImportResultBuilder.addImportResults(type, builderForType.build()));
        return relationshipImportResultBuilder.build();
    }

    private static RelationshipPropertyStore constructRelationshipPropertyStore(
        List<PropertyMapping> propertyMappings,
        List<AdjacencyProperties> adjacencyProperties,
        long relationshipCount
    ) {
        var propertyStoreBuilder = RelationshipPropertyStore.builder();

        for (int i = 0; i < propertyMappings.size(); i++) {
            var propertyMapping = propertyMappings.get(i);
            var properties = new Properties(
                adjacencyProperties.get(i),
                relationshipCount,
                propertyMapping.defaultValue().doubleValue()
            );
            var schema = RelationshipPropertySchema.of(
                propertyMapping.internalPropertyKey(),
                ValueType.DOUBLE,
                propertyMapping.defaultValue().isUserDefined()
                    ? propertyMapping.defaultValue()
                    : ValueType.DOUBLE.fallbackValue(),
                PropertyState.PERSISTENT,
                propertyMapping.aggregation()
            );
            var relationshipProperty = new RelationshipProperty(properties, schema);
            propertyStoreBuilder.putIfAbsent(propertyMapping.internalPropertyKey(), relationshipProperty);
        }

        return propertyStoreBuilder.build();
    }
}
