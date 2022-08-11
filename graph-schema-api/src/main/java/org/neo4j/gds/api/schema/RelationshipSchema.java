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

import org.immutables.builder.Builder.AccessibleFields;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.Aggregation;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ValueClass
public interface RelationshipSchema extends ElementSchema<RelationshipSchema, RelationshipType, RelationshipPropertySchema> {

    /**
     * Maps each type in this schema to a boolean which is true if and only if relationships of that type are UNDIRECTED.
     */
    Map<RelationshipType, Boolean> typeIsUndirected();

    @Override
    default RelationshipSchema filter(Set<RelationshipType> relationshipTypesToKeep) {
        return of(
            filterProperties(relationshipTypesToKeep),
            typeIsUndirected().entrySet().stream()
                .filter(kv -> relationshipTypesToKeep.contains(kv.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }

    @Override
    default RelationshipSchema union(RelationshipSchema other) {
        var mismatchTypes = this
            .typeIsUndirected()
            .entrySet()
            .stream()
            .filter(e -> other.typeIsUndirected().containsKey(e.getKey()))
            .filter(e -> other.typeIsUndirected().get(e.getKey()) != e.getValue())
            .map(e -> e.getKey().name)
            .collect(Collectors.toSet());

        if (!mismatchTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Conflicting directionality for relationship types `%s`",
                mismatchTypes
            ));
        } else {
            return of(unionSchema(other.properties()), unionTypeIsUndirectedMap(other.typeIsUndirected()));
        }
    }

    private Map<RelationshipType, Boolean> unionTypeIsUndirectedMap(Map<RelationshipType, Boolean> otherTypeIsUndirectedMap) {
        return Stream
            .concat(typeIsUndirected().entrySet().stream(), otherTypeIsUndirectedMap.entrySet().stream())
            .distinct()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    default Set<RelationshipType> availableTypes() {
        return properties().keySet();
    }

    default boolean isUndirected() {
        // a graph with no relationships is considered undirected
        // this is because algorithms such as TriangleCount are still well-defined
        // so it is the least restrictive decision
        return typeIsUndirected().values().stream().allMatch(b -> b);
    }

    static RelationshipSchema empty() {
        return builder().build();
    }

    static RelationshipSchema of(
        Map<RelationshipType, Map<String, RelationshipPropertySchema>> properties,
        Map<RelationshipType, Boolean> typeIsUndirected
    ) {
        return RelationshipSchema.builder().typeIsUndirected(typeIsUndirected).properties(properties).build();
    }

    static Builder builder() {
        return new Builder().properties(new LinkedHashMap<>()).typeIsUndirected(new LinkedHashMap<>());
    }

    @AccessibleFields
    class Builder extends ImmutableRelationshipSchema.Builder {

        public Builder addProperty(RelationshipType type, boolean isUndirected, String propertyName, ValueType valueType) {
            return addProperty(type, isUndirected, propertyName, RelationshipPropertySchema.of(propertyName, valueType));
        }

        public Builder addProperty(RelationshipType type, boolean isUndirected, String propertyName, ValueType valueType, Aggregation aggregation) {
            return addProperty(type, isUndirected, propertyName, RelationshipPropertySchema.of(propertyName, valueType, aggregation));
        }

        public Builder addProperty(
            RelationshipType type,
            boolean isUndirected,
            String propertyName,
            RelationshipPropertySchema propertySchema
        ) {
            addRelationshipType(type, isUndirected);
            this.properties.get(type).put(propertyName, propertySchema);
            return this;
        }

        public Builder addRelationshipType(RelationshipType type, boolean isUndirected) {
            this.properties.computeIfAbsent(type, ignore -> new LinkedHashMap<>());
            this.typeIsUndirected.putIfAbsent(type, isUndirected);
            return this;
        }

        public Builder removeRelationshipType(RelationshipType type) {
            this.properties.remove(type);
            this.typeIsUndirected.remove(type);
            return this;
        }
    }
}
