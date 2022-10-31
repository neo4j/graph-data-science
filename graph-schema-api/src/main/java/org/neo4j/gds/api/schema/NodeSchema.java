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
package org.neo4j.gds.api.schema;

import org.immutables.builder.Builder.AccessibleFields;
import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ValueClass
public interface NodeSchema extends ElementSchema<NodeSchema, NodeLabel, PropertySchema> {

    @Value.Derived
    default Set<NodeLabel> availableLabels() {
        return properties().keySet();
    }

    default boolean containsOnlyAllNodesLabel() {
        return availableLabels().size() == 1 && availableLabels().contains(NodeLabel.ALL_NODES);
    }

    default NodeSchema filter(Set<NodeLabel> labelsToKeep) {
        return of(filterProperties(labelsToKeep));
    }

    @Override
    default NodeSchema union(NodeSchema other) {
        return of(unionSchema(other.properties()));
    }

    @Value.Derived
    default Map<String, Object> toMap() {
        return properties().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().name,
            entry -> entry
                .getValue()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        innerEntry -> GraphSchema.forPropertySchema(innerEntry.getValue())
                    )
                )
        ));
    }

    static NodeSchema of(Map<NodeLabel, Map<String, PropertySchema>> properties) {
        if (properties.isEmpty()) {
            return empty();
        }
        return NodeSchema.builder().properties(properties).build();
    }

    static NodeSchema empty() {
        return builder().build();
    }

    static Builder builder() {
        return new Builder().properties(new LinkedHashMap<>());
    }

    @AccessibleFields
    class Builder extends ImmutableNodeSchema.Builder {

        public Builder addProperty(NodeLabel key, String propertyName, ValueType valueType) {
            return addProperty(
                key,
                propertyName,
                PropertySchema.of(
                    propertyName,
                    valueType,
                    valueType.fallbackValue(),
                    PropertyState.PERSISTENT
                )
            );
        }

        public Builder addProperty(NodeLabel key, String propertyName, PropertySchema propertySchema) {
            this.properties
                .computeIfAbsent(key, ignore -> new LinkedHashMap<>())
                .put(propertyName, propertySchema);

            return this;
        }

        public Builder addLabel(NodeLabel key) {
            this.properties.putIfAbsent(key, new LinkedHashMap<>());
            return this;
        }

        public Builder removeProperty(String propertyName) {
            this.properties.forEach((label, propertyMappings) -> {
                propertyMappings.remove(propertyName);
            });
            return this;
        }

        @Override
        public NodeSchema build() {
            if (this.properties.isEmpty()) {
                addLabel(NodeLabel.ALL_NODES);
            }
            return super.build();
        }
    }
}
