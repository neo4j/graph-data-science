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
package org.neo4j.graphalgo.api.schema;

import org.immutables.builder.Builder.AccessibleFields;
import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

    static NodeSchema of(Map<NodeLabel, Map<String, PropertySchema>> properties) {
        return NodeSchema.builder().properties(properties).build();
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
                PropertySchema.of(propertyName,
                    valueType,
                    valueType.fallbackValue(),
                    GraphStore.PropertyState.PERSISTENT
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
            this.properties.putIfAbsent(key, Collections.emptyMap());
            return this;
        }
    }
}
