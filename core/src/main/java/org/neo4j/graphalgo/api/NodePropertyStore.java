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
package org.neo4j.graphalgo.api;

import org.neo4j.gds.annotation.ValueClass;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ValueClass
public interface NodePropertyStore {

    Map<String, NodeProperty> nodeProperties();

    default Map<String, NodeProperties> nodePropertyValues() {
        return nodeProperties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().values()));
    }

    default NodeProperty get(String propertyKey) {
        return nodeProperties().get(propertyKey);
    }

    default boolean isEmpty() {
        return nodeProperties().isEmpty();
    }

    default Set<String> keySet() {
        return nodeProperties().keySet();
    }

    default boolean containsKey(String propertyKey) {
        return nodeProperties().containsKey(propertyKey);
    }

    static NodePropertyStore empty() {
        return ImmutableNodePropertyStore.of(Collections.emptyMap());
    }

    static Builder builder() {
        // need to initialize with empty map due to `deferCollectionAllocation = true`
        return new Builder().nodeProperties(Collections.emptyMap());
    }

    @org.immutables.builder.Builder.AccessibleFields
    final class Builder extends ImmutableNodePropertyStore.Builder {

        public Builder putIfAbsent(String propertyKey, NodeProperty nodeProperty) {
            nodeProperties.putIfAbsent(propertyKey, nodeProperty);
            return this;
        }

        public Builder removeProperty(String propertyKey) {
            nodeProperties.remove(propertyKey);
            return this;
        }
    }
}
