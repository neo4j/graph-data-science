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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.NodeSchema;

import java.util.Map;

public record Nodes(NodeSchema schema, IdMap idMap, NodePropertyStore properties) {

    public static Nodes of(
        IdMap idMap,
        Map<NodeLabel, PropertyMappings> propertyMappings,
        Map<PropertyMapping, NodePropertyValues> propertyValues,
        PropertyState propertyState
    ) {
        var nodeSchemaBuilder = NodeSchema.builder();
        var nodePropertyStoreBuilder = NodePropertyStore.builder();

        propertyMappings.forEach(((nodeLabel, mappings) -> {
            if (mappings.mappings().isEmpty()) {
                nodeSchemaBuilder.addLabel(nodeLabel.name());

            } else {
                for (var propertyMapping : mappings.mappings()) {
                    var nodePropertyValues = propertyValues.get(propertyMapping);
                    // The default value is either overridden by the user
                    // or inferred from the actual property value.
                    var defaultValue = propertyMapping.defaultValue().isUserDefined()
                        ? propertyMapping.defaultValue()
                        : nodePropertyValues.valueType().fallbackValue();
                    var nodeProperty = NodeProperty.of(
                        propertyMapping.propertyKey(),
                        propertyState,
                        nodePropertyValues,
                        defaultValue
                    );

                    nodeSchemaBuilder.addProperty(nodeLabel.name(), nodeProperty.propertySchema());
                    nodePropertyStoreBuilder.putProperty(nodeProperty.propertySchema().key(), nodeProperty);
                }
            }
        }));

        return new Nodes(nodeSchemaBuilder.build(), idMap, nodePropertyStoreBuilder.build());
    }
}
