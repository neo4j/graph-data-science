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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class MutableNodeSchema implements NodeSchema {

    private final Map<NodeLabel, MutableNodeSchemaEntry> entries;

    public static MutableNodeSchema empty() {
        return new MutableNodeSchema(new LinkedHashMap<>());
    }

    private MutableNodeSchema(Map<NodeLabel, MutableNodeSchemaEntry> entries) {
        this.entries=entries;
    }

    public static MutableNodeSchema from(NodeSchema fromSchema) {
        var nodeSchema = MutableNodeSchema.empty();
        fromSchema.entries().forEach(fromEntry -> nodeSchema.set(MutableNodeSchemaEntry.from(fromEntry)));

        return nodeSchema;
    }

    @Override
    public Set<NodeLabel> availableLabels() {
        return entries.keySet();
    }

    @Override
    public boolean containsOnlyAllNodesLabel() {
        return availableLabels().size() == 1 && availableLabels().contains(NodeLabel.ALL_NODES);
    }

    @Override
    public MutableNodeSchema filter(Set<NodeLabel> labelsToKeep) {
        return new MutableNodeSchema(entries
            .entrySet()
            .stream()
            .filter(e -> labelsToKeep.contains(e.getKey()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> MutableNodeSchemaEntry.from(entry.getValue())
            )));
    }

    @Override
    public MutableNodeSchema union(NodeSchema other) {
        return new MutableNodeSchema(unionEntries(other));
    }

    @Override
    public Collection<MutableNodeSchemaEntry> entries() {
        return entries.values();
    }

    @Override
    public MutableNodeSchemaEntry get(NodeLabel identifier) {
        return entries.get(identifier);
    }

    public void set(MutableNodeSchemaEntry entry) {
        entries.put(entry.identifier(), entry);
    }
    public void remove(NodeLabel identifier) {
        entries.remove(identifier);
    }

    public MutableNodeSchemaEntry getOrCreateLabel(NodeLabel key) {
        return this.entries.computeIfAbsent(key, (__) -> new MutableNodeSchemaEntry(key));
    }

    public MutableNodeSchema addLabel(NodeLabel nodeLabel) {
        getOrCreateLabel(nodeLabel);
        return this;
    }

    public MutableNodeSchema addLabel(NodeLabel nodeLabel, Map<String, PropertySchema> nodeProperties) {
        var nodeSchemaEntry = getOrCreateLabel(nodeLabel);
        nodeProperties.forEach(nodeSchemaEntry::addProperty);
        return this;
    }

    public MutableNodeSchema addProperty(NodeLabel nodeLabel, String propertyName, PropertySchema propertySchema) {
        getOrCreateLabel(nodeLabel).addProperty(propertyName, propertySchema);
        return this;
    }

    public MutableNodeSchema addProperty(NodeLabel nodeLabel, String propertyKey, ValueType valueType) {
        getOrCreateLabel(nodeLabel).addProperty(propertyKey, valueType);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableNodeSchema that = (MutableNodeSchema) o;

        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}
