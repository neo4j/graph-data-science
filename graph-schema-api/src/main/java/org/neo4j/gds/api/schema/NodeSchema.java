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
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class NodeSchema extends ElementSchema<NodeSchema, NodeLabel, NodeSchemaEntry, PropertySchema> {

    public static NodeSchema empty() {
        return new NodeSchema(new LinkedHashMap<>());
    }

    private NodeSchema(Map<NodeLabel, NodeSchemaEntry> entries) {
        super(entries);
    }

    public static NodeSchema from(NodeSchema fromSchema) {
        var nodeSchema = NodeSchema.empty();
        fromSchema.entries().forEach(fromEntry -> nodeSchema.set(NodeSchemaEntry.from(fromEntry)));

        return nodeSchema;
    }

    public Set<NodeLabel> availableLabels() {
        return entries.keySet();
    }

    public boolean containsOnlyAllNodesLabel() {
        return availableLabels().size() == 1 && availableLabels().contains(NodeLabel.ALL_NODES);
    }

    public NodeSchema filter(Set<NodeLabel> labelsToKeep) {
        return new NodeSchema(entries
            .entrySet()
            .stream()
            .filter(e -> labelsToKeep.contains(e.getKey()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> NodeSchemaEntry.from(entry.getValue())
            )));
    }

    @Override
    public NodeSchema union(NodeSchema other) {
        return new NodeSchema(unionEntries(other));
    }


    public NodeSchemaEntry getOrCreateLabel(NodeLabel key) {
        return this.entries.computeIfAbsent(key, (__) -> new NodeSchemaEntry(key));
    }

    public NodeSchema addLabel(NodeLabel nodeLabel) {
        getOrCreateLabel(nodeLabel);
        return this;
    }

    public NodeSchema addProperty(NodeLabel nodeLabel, String propertyName, PropertySchema propertySchema) {
        getOrCreateLabel(nodeLabel).addProperty(propertyName, propertySchema);
        return this;
    }

    public NodeSchema addProperty(NodeLabel nodeLabel, String propertyKey, ValueType valueType) {
        getOrCreateLabel(nodeLabel).addProperty(propertyKey, valueType);
        return this;
    }

    public void copyUnionPropertiesToLabel(NodeLabel nodeLabel) {
        assert availableLabels().contains(nodeLabel) : "The node label should be in the schema before we can add properties to it";
        var nodeSchemaEntry = get(nodeLabel);
        unionProperties().forEach(nodeSchemaEntry::addProperty);
    }
}
