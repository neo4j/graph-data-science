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

import org.neo4j.gds.ElementIdentifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ElementSchema<
        SELF extends ElementSchema<SELF, ELEMENT_IDENTIFIER, ENTRY, PROPERTY_SCHEMA>,
        ELEMENT_IDENTIFIER extends ElementIdentifier,
        ENTRY extends ElementSchemaEntry<ENTRY, ELEMENT_IDENTIFIER, PROPERTY_SCHEMA>,
        PROPERTY_SCHEMA extends PropertySchema
    > {

    protected final Map<ELEMENT_IDENTIFIER, ENTRY> entries;

    ElementSchema(Map<ELEMENT_IDENTIFIER, ENTRY> entries) {
        this.entries = entries;
    }

    abstract SELF filter(Set<ELEMENT_IDENTIFIER> elementIdentifiersToKeep);

    abstract SELF union(SELF other);

    public Collection<ENTRY> entries() {
        return this.entries.values();
    }

    public void set(ENTRY entry) {
        entries.put(entry.identifier(), entry);
    }
    public ENTRY get(ELEMENT_IDENTIFIER identifier) {
        return entries.get(identifier);
    }

    public void remove(ELEMENT_IDENTIFIER identifier) {
        entries.remove(identifier);
    }

    public Set<String> allProperties() {
        return this.entries
            .values()
            .stream()
            .flatMap(entry -> entry.properties().keySet().stream())
            .collect(Collectors.toSet());
    }

    public Set<String> allProperties(ELEMENT_IDENTIFIER elementIdentifier) {
        return Optional.ofNullable(this.entries.get(elementIdentifier))
            .map(entry -> entry.properties().keySet())
            .orElse(Set.of());
    }

    public boolean hasProperties() {
        return entries.values().stream().anyMatch(entry -> !entry.properties().isEmpty());
    }

    public boolean hasProperty(ELEMENT_IDENTIFIER elementIdentifier, String propertyKey) {
        return entries.containsKey(elementIdentifier) && entries
            .get(elementIdentifier)
            .properties()
            .containsKey(propertyKey);
    }

    public List<PROPERTY_SCHEMA> propertySchemasFor(ELEMENT_IDENTIFIER elementIdentifier) {
        return Optional
            .ofNullable(entries.get(elementIdentifier))
            .map(entry -> entry.properties().values())
            .map(ArrayList::new)
            .orElse(new ArrayList<>());
    }

    /**
     * Returns a union of all properties in the given schema.
     */
    public Map<String, PROPERTY_SCHEMA> unionProperties() {
        return entries
            .values()
            .stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (leftSchema, rightSchema) -> leftSchema));
    }


    public Map<String, Object> toMap() {
        return entries
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().name, entry -> entry.getValue().toMap()));
    }

    Map<ELEMENT_IDENTIFIER, ENTRY> filterByElementIdentifier(Set<ELEMENT_IDENTIFIER> elementIdentifiersToKeep) {
        return entries
            .entrySet()
            .stream()
            .filter(e -> elementIdentifiersToKeep.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<ELEMENT_IDENTIFIER, ENTRY> unionEntries(SELF other) {
        return Stream
            .concat(entries.entrySet().stream(), other.entries.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, ElementSchemaEntry::union));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElementSchema<?, ?, ?, ?> that = (ElementSchema<?, ?, ?, ?>) o;

        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return toMap().toString();
    }
}
