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

import org.immutables.value.Value;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.ImmutableNodeProperty;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.ImmutablePropertySchema;
import org.neo4j.gds.api.schema.NodeSchema;

import java.util.Map;

@ValueClass
public interface Nodes {

    @Value.Default
    default NodeSchema schema() {
        return NodeSchema.empty();
    }

    IdMap idMap();

    @Value.Default
    default NodePropertyStore properties() {
        return NodePropertyStore.empty();
    }

    @TestOnly
    static Nodes of(IdMap idmap) {
        return ImmutableNodes.of(NodeSchema.empty(), idmap, NodePropertyStore.empty());
    }

    static Nodes of(
        IdMap idMap,
        Map<NodeLabel, PropertyMappings> propertyMappingsByLabel,
        Map<PropertyMapping, NodePropertyValues> properties,
        PropertyState propertyState
    ) {
        var nodeSchema = NodeSchema.empty();
        var nodePropertyStoreBuilder = NodePropertyStore.builder();
        propertyMappingsByLabel.forEach(((nodeLabel, propertyMappings) -> {
            if (propertyMappings.mappings().isEmpty()) {
                nodeSchema.addLabel(nodeLabel);
            } else {
                propertyMappings.mappings().forEach(propertyMapping -> {
                    var nodePropertyValues = properties.get(propertyMapping);
                    // The default value is either overridden by the user
                    // or inferred from the actual property value.
                    var defaultValue = propertyMapping.defaultValue().equals(DefaultValue.DEFAULT)
                        ? DefaultValue.of(nodePropertyValues.valueType())
                        : propertyMapping.defaultValue();
                    var propertySchema = ImmutablePropertySchema.builder()
                        .key(propertyMapping.propertyKey())
                        .valueType(nodePropertyValues.valueType())
                        .defaultValue(defaultValue)
                        .state(propertyState)
                        .build();

                    nodeSchema.addProperty(nodeLabel, propertySchema.key(), propertySchema);
                    nodePropertyStoreBuilder.putProperty(
                        propertySchema.key(),
                        ImmutableNodeProperty.of(nodePropertyValues, propertySchema)
                    );
                });
            }
        }));

        return ImmutableNodes.of(nodeSchema, idMap, nodePropertyStoreBuilder.build());
    }

    static Nodes of(
        NodeSchema nodeSchema,
        IdMap idMap,
        Map<PropertyMapping, NodePropertyValues> properties,
        PropertyState propertyState
    ) {
        var propertyStoreBuilder = NodePropertyStore.builder();
        properties.forEach((mapping, nodeProperties) -> propertyStoreBuilder.putProperty(
            mapping.propertyKey(),
            NodeProperty.of(
                mapping.propertyKey(),
                propertyState,
                nodeProperties,
                mapping.defaultValue().isUserDefined()
                    ? mapping.defaultValue()
                    : nodeProperties.valueType().fallbackValue()
            )
        ));

        return ImmutableNodes.of(nodeSchema, idMap, propertyStoreBuilder.build());
    }
}
