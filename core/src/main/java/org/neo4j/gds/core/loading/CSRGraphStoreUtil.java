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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.ValueTypes;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.graph.GraphPropertyStore;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.construction.RelationshipsAndDirection;
import org.neo4j.values.storable.NumberType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CSRGraphStoreUtil {

    public static CSRGraphStore createFromGraph(
        DatabaseId databaseId,
        HugeGraph graph,
        String relationshipTypeString,
        Optional<String> relationshipPropertyKey,
        int concurrency
    ) {
        var relationshipType = RelationshipType.of(relationshipTypeString);
        var relationships = graph.relationships();
        Direction direction = graph.schema().isUndirected() ? Direction.UNDIRECTED : Direction.DIRECTED;

        var relationshipSchema = RelationshipSchema.empty();
        var entry = relationshipSchema.getOrCreateRelationshipType(relationshipType, direction);

        relationshipPropertyKey.ifPresent(property -> {

            if (!graph.hasRelationshipProperty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Expected relationship property '%s', but graph has none.",
                    property
                ));
            }

            entry.addProperty(
                property,
                ValueType.DOUBLE,
                PropertyState.PERSISTENT
            );
        });

        var topology = Map.of(relationshipType, relationships.topology());
        var nodeProperties = constructNodePropertiesFromGraph(graph);
        var relationshipProperties = constructRelationshipPropertiesFromGraph(
            graph,
            relationshipPropertyKey,
            relationships,
            relationshipType
        );

        var schema = GraphSchema.of(NodeSchema.from(graph.schema().nodeSchema()), relationshipSchema, Map.of());

        return new CSRGraphStore(
            databaseId,
            // TODO: is it correct that we only use this for generated graphs?
            ImmutableStaticCapabilities.of(false),
            schema,
            graph.idMap(),
            nodeProperties,
            topology,
            relationshipProperties,
            GraphPropertyStore.empty(),
            concurrency
        );
    }

    @NotNull
    private static NodePropertyStore constructNodePropertiesFromGraph(HugeGraph graph) {
        var nodePropertyStoreBuilder = NodePropertyStore.builder();

        graph
            .schema()
            .nodeSchema()
            .unionProperties()
            .forEach((propertyKey, propertySchema) -> nodePropertyStoreBuilder.putIfAbsent(
                propertyKey,
                NodeProperty.of(
                    propertyKey,
                    propertySchema.state(),
                    graph.nodeProperties(propertyKey),
                    propertySchema.defaultValue()
                )
            ));

        return nodePropertyStoreBuilder.build();
    }

    @NotNull
    private static Map<RelationshipType, RelationshipPropertyStore> constructRelationshipPropertiesFromGraph(
        Graph graph,
        Optional<String> relationshipProperty,
        Relationships relationships,
        RelationshipType relationshipType
    ) {
        Map<RelationshipType, RelationshipPropertyStore> relationshipProperties = Collections.emptyMap();
        if (relationshipProperty.isPresent() && relationships.properties().isPresent()) {
            Map<String, RelationshipPropertySchema> relationshipPropertySchemas = graph
                .schema()
                .relationshipSchema()
                .get(relationshipType)
                .properties();

            if (relationshipPropertySchemas.size() != 1) {
                throw new IllegalStateException(formatWithLocale(
                    "Relationship schema is expected to have exactly one property but had %s",
                    relationshipPropertySchemas.size()
                ));
            }

            RelationshipPropertySchema relationshipPropertySchema = relationshipPropertySchemas
                .values()
                .stream()
                .findFirst()
                .orElseThrow();

            String propertyKey = relationshipProperty.get();
            relationshipProperties = singletonMap(
                relationshipType,
                RelationshipPropertyStore.builder().putIfAbsent(
                    propertyKey,
                    RelationshipProperty.of(
                        propertyKey,
                        NumberType.FLOATING_POINT,
                        relationshipPropertySchema.state(),
                        relationships.properties().orElseThrow(),
                        relationshipPropertySchema.defaultValue().isUserDefined()
                            ? relationshipPropertySchema.defaultValue()
                            : ValueTypes.fromNumberType(NumberType.FLOATING_POINT).fallbackValue(),
                        relationshipPropertySchema.aggregation()
                    )
                ).build()
            );
        }
        return relationshipProperties;
    }

    public static void extractNodeProperties(
        GraphStoreBuilder graphStoreBuilder,
        Function<String, PropertySchema> nodeSchema,
        Map<String, NodePropertyValues> nodeProperties
    ) {
        NodePropertyStore.Builder propertyStoreBuilder = NodePropertyStore.builder();
        nodeProperties.forEach((propertyKey, propertyValues) -> {
            var propertySchema = nodeSchema.apply(propertyKey);
            propertyStoreBuilder.putIfAbsent(
                propertyKey,
                NodeProperty.of(
                    propertyKey,
                    propertySchema.state(),
                    propertyValues,
                    propertySchema.defaultValue()
                )
            );
        });
        graphStoreBuilder.nodePropertyStore(propertyStoreBuilder.build());
    }

    public static RelationshipPropertyStore buildRelationshipPropertyStore(
        List<RelationshipsAndDirection> relationships,
        List<RelationshipPropertySchema> relationshipPropertySchemas
    ) {
        assert relationships.size() >= relationshipPropertySchemas.size();

        var propertyStoreBuilder = RelationshipPropertyStore.builder();

        for (int i = 0; i < relationshipPropertySchemas.size(); i++) {
            var relationship = relationships.get(i);
            var relationshipPropertySchema = relationshipPropertySchemas.get(i);
            relationship.relationships().properties().ifPresent(properties -> {

                propertyStoreBuilder.putIfAbsent(relationshipPropertySchema.key(), RelationshipProperty.of(
                        relationshipPropertySchema.key(),
                        NumberType.FLOATING_POINT,
                        relationshipPropertySchema.state(),
                        properties,
                        relationshipPropertySchema.defaultValue(),
                        relationshipPropertySchema.aggregation()
                    )
                );
            });
        }

        return propertyStoreBuilder.build();
    }

    public static GraphSchema computeGraphSchema(
        IdMapAndProperties idMapAndProperties,
        Function<NodeLabel, Collection<String>> propertiesByLabel,
        RelationshipImportResult relationshipImportResult
    ) {
        var properties = idMapAndProperties.properties().properties();

        var nodeSchema = NodeSchema.empty();
        for (var label : idMapAndProperties.idMap().availableNodeLabels()) {
            var entry = nodeSchema.getOrCreateLabel(label);
            for (var propertyKey : propertiesByLabel.apply(label)) {
                entry.addProperty(
                    propertyKey,
                    properties.get(propertyKey).propertySchema()
                );
            }
        }
        idMapAndProperties.idMap().availableNodeLabels().forEach(nodeSchema::getOrCreateLabel);

        var relationshipSchema = RelationshipSchema.empty();
        relationshipImportResult
            .properties()
            .forEach((relType, propertyStore) -> {
                var entry = relationshipSchema.getOrCreateRelationshipType(
                    relType,
                    relationshipImportResult.directions().get(relType)
                );

                propertyStore
                    .relationshipProperties()
                    .forEach((propertyKey, propertyValues) -> entry.addProperty(
                        propertyKey,
                        propertyValues.propertySchema()
                    ));
            });

        relationshipImportResult
            .relationships()
            .keySet()
            .forEach(type -> {
                relationshipSchema.getOrCreateRelationshipType(
                    type,
                    relationshipImportResult.directions().get(type)
                );
            });

        return GraphSchema.of(
            nodeSchema,
            relationshipSchema,
            Map.of()
        );
    }


    private CSRGraphStoreUtil() {}
}
