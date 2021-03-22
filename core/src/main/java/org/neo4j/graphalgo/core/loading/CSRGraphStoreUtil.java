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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperty;
import org.neo4j.graphalgo.api.NodePropertyStore;
import org.neo4j.graphalgo.api.RelationshipProperty;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.RelationshipPropertySchema;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.storable.NumberType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class CSRGraphStoreUtil {

    public static CSRGraphStore createFromGraph(
        NamedDatabaseId databaseId,
        HugeGraph graph,
        String relationshipTypeString,
        Optional<String> relationshipProperty,
        int concurrency,
        AllocationTracker tracker
    ) {
        Relationships relationships = graph.relationships();

        var relationshipType = RelationshipType.of(relationshipTypeString);
        var topology = Map.of(relationshipType, relationships.topology());

        var nodeProperties = constructNodePropertiesFromGraph(graph);
        var relationshipProperties = constructRelationshipPropertiesFromGraph(
            graph,
            relationshipProperty,
            relationships,
            relationshipType
        );

        return new CSRGraphStore(
            databaseId,
            graph.idMap(),
            nodeProperties,
            topology,
            relationshipProperties,
            concurrency,
            tracker
        );
    }

    @NotNull
    private static Map<NodeLabel, NodePropertyStore> constructNodePropertiesFromGraph(HugeGraph graph) {
        Map<NodeLabel, NodePropertyStore> nodePropertyStores = new HashMap<>();
        graph
            .schema()
            .nodeSchema()
            .properties()
            .forEach((nodeLabel, propertySchemas) -> {
                var nodePropertyStoreBuilder = NodePropertyStore.builder();

                propertySchemas.forEach((propertyKey, propertySchema) -> {
                    nodePropertyStoreBuilder.putIfAbsent(
                        propertyKey,
                        NodeProperty.of(propertyKey,
                            propertySchema.state(),
                            graph.nodeProperties(propertyKey),
                            propertySchema.defaultValue()
                        )
                    );
                });
                nodePropertyStores.put(nodeLabel, nodePropertyStoreBuilder.build());

            });

        return nodePropertyStores;
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
                .properties()
                .get(relationshipType);

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
                .get();

            String propertyKey = relationshipProperty.get();
            relationshipProperties = singletonMap(
                relationshipType,
                RelationshipPropertyStore.builder().putIfAbsent(
                    propertyKey,
                    RelationshipProperty.of(
                        propertyKey,
                        NumberType.FLOATING_POINT,
                        relationshipPropertySchema.state(),
                        relationships.properties().get(),
                        relationshipPropertySchema.defaultValue().isUserDefined()
                            ? relationshipPropertySchema.defaultValue()
                            : ValueType.fromNumberType(NumberType.FLOATING_POINT).fallbackValue(),
                        relationshipPropertySchema.aggregation()
                    )
                ).build()
            );
        }
        return relationshipProperties;
    }

    private CSRGraphStoreUtil() {}
}
