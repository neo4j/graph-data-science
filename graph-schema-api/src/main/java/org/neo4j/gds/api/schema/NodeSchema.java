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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.utils.StringFormatting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.flatMapping;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public record NodeSchema(Map<NodeLabel, List<PropertySchema>> entries) {

    public NodeSchema {
        // Assumes NodeLabel and PropertySchema are immutable classes
        entries = entries.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> List.copyOf(entry.getValue())
            ));
    }

    static NodeSchema of(Map<NodeLabel, List<PropertySchema>> entries) {
        return new NodeSchema(entries);
    }

    public Optional<DefaultValue> getDefaultValueFor(String label, String propertyKey) {
        return entries.getOrDefault(NodeLabel.of(label), List.of()).stream()
            .filter(propertySchema -> propertySchema.key().equals(propertyKey))
            .findFirst()
            .map(PropertySchema::defaultValue);
    }

    public NodeSchema filter(Set<NodeLabel> labels) {
        return NodeSchema.of(entries.entrySet().stream()
            .filter((entry) -> labels.contains(entry.getKey()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ))
        );
    }

    public NodeSchema union(NodeSchema other) {
        return NodeSchema.builder()
            .addSchema(this)
            .addSchema(other)
            .build();
    }

    public boolean hasProperties() {
        return entries.values().stream()
            .anyMatch(x -> !x.isEmpty());
    }

    public boolean hasProperty(String label, String propertyKey) {
        var nodeLabel = NodeLabel.of(label);
        if (!entries.containsKey(nodeLabel)) {
            return false;
        }
        return entries.get(nodeLabel).stream()
            .anyMatch(propertySchema -> propertySchema.key().equals(propertyKey));
    }

    public Set<NodeLabel> availableLabels() {
        return entries.keySet();
    }

    public boolean containsOnlyAllNodesLabel() {
        return entries.keySet().equals(Set.of(NodeLabel.ALL_NODES));
    }

    public List<PropertySchema> allProperties() {
        return entries.values().stream()
            .flatMap(List::stream)
            .distinct()
            .toList();
    }

    public Set<String> allPropertyKeysWithLabel(String label) {
        return entries.getOrDefault(NodeLabel.of(label), List.of()).stream()
            .map(PropertySchema::key)
            .collect(Collectors.toSet());
    }

    public Map<String, PropertySchema> properties() {
        return entries.values().stream()
            .flatMap(List::stream)
            .collect(
                Collectors.toMap(
                    PropertySchema::key,
                    propertySchema -> propertySchema,
                    (left, right) -> left
                )
            );
    }

    public Map<String, PropertySchema> propertiesForLabel(NodeLabel label) {
        return entries.getOrDefault(label, List.of()).stream()
            .collect(
                Collectors.toMap(
                    PropertySchema::key,
                    propertySchema -> propertySchema,
                    (left, right) -> left
                )
            );
    }

    public static NodeSchema empty() {
        return new NodeSchema(Collections.emptyMap());
    }

    /**
     * Builder
     */
    public static NodeSchemaBuilder builder() {
        return new NodeSchemaBuilder();
    }

    public static final class NodeSchemaBuilder {
        private final List<SchemaRow> rows;

        private NodeSchemaBuilder() {
            rows = new ArrayList<>();
        }

        public NodeSchemaBuilder addLabel(String nodeLabel) {
            rows.add(new SchemaRow(nodeLabel, Optional.empty()));
            return this;
        }

        public NodeSchemaBuilder addProperty(String nodeLabel, String propertyKey, ValueType valueType) {
            rows.add(new SchemaRow(nodeLabel, Optional.of(PropertySchema.of(propertyKey, valueType))));
            return this;
        }

        public NodeSchemaBuilder addProperty(
            String nodeLabel,
            String propertyKey,
            ValueType valueType,
            DefaultValue defaultValue,
            PropertyState propertyState
        ) {
            var propertySchema = PropertySchema.of(propertyKey, valueType, defaultValue, propertyState);
            rows.add(new SchemaRow(nodeLabel, Optional.of(propertySchema)));
            return this;
        }

        public NodeSchemaBuilder addSchema(NodeSchema nodeSchema) {
            nodeSchema.entries()
                .forEach(
                    (nodeLabel, propertySchemas) -> {
                        if (propertySchemas.isEmpty()) {
                            addLabel(nodeLabel.name());
                        } else {
                            propertySchemas.forEach(propertySchema -> addProperty(nodeLabel, propertySchema));
                        }
                    }
                );
            return this;
        }

        public NodeSchemaBuilder addBuilder(NodeSchemaBuilder builder) {
            rows.addAll(builder.rows);
            return this;
        }

        private void addProperty(NodeLabel nodeLabel, PropertySchema propertySchema) {
            addProperty(
                nodeLabel.name(),
                propertySchema.key(),
                propertySchema.valueType(),
                propertySchema.defaultValue(),
                propertySchema.state()
            );
        }

        public NodeSchema build() {
            return NodeSchema.of(rows.stream()
                .collect(
                    groupingBy(
                        row -> NodeLabel.of(row.nodeLabel),
                        collectingAndThen(
                            flatMapping(
                                row -> row.maybePropertySchema.stream(),
                                toCollection(() -> new TreeSet<>(NodeSchemaBuilder::comparator))
                            ),
                            set -> set.stream().toList()
                        )
                    )
                ));
        }

        private static int comparator(PropertySchema left, PropertySchema right) {
            var sameKey = left.key().equals(right.key());
            var sameType = left.valueType() == right.valueType();
            if (sameKey && !sameType) {
                throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                    "Combining schema entries with value type %s and %s is not supported.",
                    left.toString(),
                    right.toString()
                ));
            }

            return left.key().compareTo(right.key());
        }

        private record SchemaRow(String nodeLabel,  Optional<PropertySchema> maybePropertySchema) {}
    }
}
