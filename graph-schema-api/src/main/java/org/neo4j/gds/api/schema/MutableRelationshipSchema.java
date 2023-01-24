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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MutableRelationshipSchema implements RelationshipSchema {

    private final Map<RelationshipType, MutableRelationshipSchemaEntry> entries;

    public static MutableRelationshipSchema empty() {
        return new MutableRelationshipSchema(new LinkedHashMap<>());
    }

    public MutableRelationshipSchema(Map<RelationshipType, MutableRelationshipSchemaEntry> entries) {
        this.entries = entries;
    }

    public static MutableRelationshipSchema from(RelationshipSchema fromSchema) {
        var relationshipSchema = MutableRelationshipSchema.empty();
        fromSchema.entries().forEach(fromEntry -> relationshipSchema.set(MutableRelationshipSchemaEntry.from(fromEntry)));

        return relationshipSchema;
    }

    @Override
    public MutableRelationshipSchema filter(Set<RelationshipType> relationshipTypesToKeep) {
        return new MutableRelationshipSchema(entries
            .entrySet()
            .stream()
            .filter(e -> relationshipTypesToKeep.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> MutableRelationshipSchemaEntry.from(entry.getValue()))));
    }

    @Override
    public MutableRelationshipSchema union(RelationshipSchema other) {
        return new MutableRelationshipSchema(unionEntries(other));
    }

    @Override
    public Collection<MutableRelationshipSchemaEntry> entries() {
        return entries.values();
    }

    @Override
    public MutableRelationshipSchemaEntry get(RelationshipType identifier) {
        return entries.get(identifier);
    }

    @Override
    public Set<RelationshipType> availableTypes() {
        return entries.keySet();
    }

    @Override
    public boolean isUndirected() {
        // a graph with no relationships is considered undirected
        // this is because algorithms such as TriangleCount are still well-defined
        // so it is the least restrictive decision
        return entries.values().stream().allMatch(MutableRelationshipSchemaEntry::isUndirected);
    }

    @Override
    public boolean isUndirected(RelationshipType type) {
        return entries.get(type).isUndirected();
    }

    // TODO: remove
    @Override
    public Map<RelationshipType, Direction> directions() {
        return availableTypes()
            .stream()
            .collect(Collectors.toMap(
                relationshipType -> relationshipType,
                relationshipType -> entries.get(relationshipType).direction()
            ));
    }

    @Override
    public Object toMapOld() {
        return entries()
            .stream()
            .collect(Collectors.toMap(e -> e.identifier().name(),
                e -> e
                    .properties()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        innerEntry -> GraphSchema.forPropertySchema(innerEntry.getValue())
                    ))
            ));
    }

    public void set(MutableRelationshipSchemaEntry entry) {
        entries.put(entry.identifier(), entry);
    }
    public void remove(RelationshipType identifier) {
        entries.remove(identifier);
    }

    public MutableRelationshipSchemaEntry getOrCreateRelationshipType(
        RelationshipType relationshipType, Direction direction
    ) {
        return this.entries.computeIfAbsent(relationshipType,
            __ -> new MutableRelationshipSchemaEntry(relationshipType, direction)
        );
    }

    public MutableRelationshipSchema addRelationshipType(RelationshipType relationshipType, Direction direction) {
        getOrCreateRelationshipType(relationshipType, direction);
        return this;
    }

    public MutableRelationshipSchema addProperty(
        RelationshipType relationshipType,
        Direction direction,
        String propertyKey,
        RelationshipPropertySchema propertySchema
    ) {
        getOrCreateRelationshipType(relationshipType, direction).addProperty(propertyKey, propertySchema);
        return this;
    }

    public MutableRelationshipSchema addProperty(
        RelationshipType relationshipType,
        Direction direction,
        String propertyKey,
        ValueType valueType,
        PropertyState propertyState
    ) {
        getOrCreateRelationshipType(relationshipType, direction).addProperty(propertyKey, valueType, propertyState);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableRelationshipSchema that = (MutableRelationshipSchema) o;

        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}
