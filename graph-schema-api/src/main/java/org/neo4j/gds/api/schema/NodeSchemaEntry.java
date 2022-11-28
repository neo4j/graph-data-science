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
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeSchemaEntry extends ElementSchemaEntry<NodeSchemaEntry, NodeLabel, PropertySchema> {

    NodeSchemaEntry(NodeLabel identifier) {
        this(identifier, new LinkedHashMap<>());
    }

    public NodeSchemaEntry(NodeLabel identifier, Map<String, PropertySchema> properties) {
        super(identifier, properties);
    }

    static NodeSchemaEntry from(NodeSchemaEntry fromEntry) {
        return new NodeSchemaEntry(fromEntry.identifier(), new HashMap<>(fromEntry.properties()));
    }

    @Override
    NodeSchemaEntry union(NodeSchemaEntry other) {
        if (!other.identifier().equals(this.identifier())) {
            throw new UnsupportedOperationException(
                formatWithLocale(
                    "Cannot union node schema entries with different node labels %s and %s",
                    this.identifier(),
                    other.identifier()
                )
            );
        }

        return new NodeSchemaEntry(this.identifier(), unionProperties(other.properties));
    }

    public NodeSchemaEntry addProperty(String propertyName, ValueType valueType) {
        return addProperty(
            propertyName,
            PropertySchema.of(
                propertyName,
                valueType,
                valueType.fallbackValue(),
                PropertyState.PERSISTENT
            )
        );
    }

    public NodeSchemaEntry addProperty(String propertyName, PropertySchema propertySchema) {
        this.properties.put(propertyName, propertySchema);
        return this;
    }

    public void removeProperty(String propertyName) {
        properties.remove(propertyName);
    }

    @Override
    Map<String, Object> toMap() {
        return properties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    innerEntry -> GraphSchema.forPropertySchema(innerEntry.getValue())
                )

            );
    }
}
