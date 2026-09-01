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
package org.neo4j.gds.api.properties.nodes;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record NodePropertyStore(Map<String, NodeProperty> properties) {

    public NodePropertyStore {
        // Ensure stored map is immutable
        properties = Collections.unmodifiableMap(properties);
    }

    public static NodePropertyStore empty() {
        return new NodePropertyStore(Collections.emptyMap());
    }

    public NodePropertyStore copyAndAdd(NodeProperty nodeProperty) {
        var copiedProperties = new LinkedHashMap<>(properties);
        copiedProperties.put(nodeProperty.key(), nodeProperty);
        return new NodePropertyStore(copiedProperties);
    }

    public NodePropertyStore copyAndRemove(String nodePropertyKey) {
        var copiedProperties = new LinkedHashMap<>(properties);
        copiedProperties.remove(nodePropertyKey);
        return new NodePropertyStore(copiedProperties);
    }

    public boolean containsKey(String propertyKey) {
        return properties().containsKey(propertyKey);
    }

    public Set<String> keySet() {
        return Collections.unmodifiableSet(properties().keySet());
    }

    public Map<String, NodePropertyValues> propertyValues() {
        return properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().values()));
    }

    public NodeProperty get(String propertyKey) {
        return properties().get(propertyKey);
    }

    public boolean isEmpty() {
        return properties().isEmpty();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        Map<String, NodeProperty> builderNodeProperties;

        private Builder() {
            this.builderNodeProperties = new LinkedHashMap<>();
        }

        public Builder putProperty(String propertyKey, NodeProperty nodeProperty) {
            this.builderNodeProperties.put(propertyKey, nodeProperty);
            return this;
        }

        public Builder putIfAbsent(String propertyKey, NodeProperty nodeProperty) {
            builderNodeProperties.putIfAbsent(propertyKey, nodeProperty);
            return this;
        }

        public Builder addAll(Builder other) {
            builderNodeProperties.putAll(other.builderNodeProperties);
            return this;
        }

        public NodePropertyStore build() {
            return new NodePropertyStore(builderNodeProperties);
        }
    }
}
