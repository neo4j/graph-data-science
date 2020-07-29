/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@ValueClass
public interface NodeSchema {
    Map<NodeLabel, Map<String, ValueType>> properties();

    default Map<String, Object> toMap() {
        return properties().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().name,
            entry -> entry
                .getValue()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    innerEntry -> GraphStoreSchema.forValueType(innerEntry.getValue()))
                )
        ));
    }

    static NodeSchema of(Map<NodeLabel, Map<String, ValueType>> properties) {
        return NodeSchema.builder().properties(properties).build();
    }

    static Builder builder() {
        return new Builder().properties(new LinkedHashMap<>());
    }

    @AccessibleFields
    class Builder extends ImmutableNodeSchema.Builder {

        public void addPropertyAndTypeForLabel(NodeLabel key, String propertyName, ValueType type) {
            this.properties
                .computeIfAbsent(key, ignore -> new LinkedHashMap<>())
                .put(propertyName, type);
        }

        public void addEmptyMapForLabelWithoutProperties(NodeLabel key) {
            this.properties.putIfAbsent(key, Collections.emptyMap());
        }
    }
}
