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
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ValueClass
public interface RelationshipSchema extends ElementSchema<RelationshipSchema, RelationshipType> {
    Map<RelationshipType, Map<String, ValueType>> properties();

    @Override
    default RelationshipSchema filter(Set<RelationshipType> relationshipTypesToKeep) {
        return of(filterProperties(relationshipTypesToKeep));
    }

    @Override
    default RelationshipSchema union(RelationshipSchema other) {
        return of(unionProperties(other.properties()));
    }

    default RelationshipSchema singleTypeAndProperty(
        RelationshipType relationshipType,
        Optional<String> maybeProperty
    ) {
        if (!properties().containsKey(relationshipType)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Relationship schema does not contain relationship type '%s'",
                relationshipType.name
            ));
        }

        maybeProperty.ifPresent(property -> {
            if (!properties().get(relationshipType).containsKey(property)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Relationship schema does not contain relationship type '%s' and property '%s",
                    relationshipType.name,
                    property
                ));
            }
        });

        if (maybeProperty.isPresent()) {
            return RelationshipSchema
                .builder()
                .addProperty(relationshipType, maybeProperty.get(), ValueType.DOUBLE)
                .build();
        } else {
            return RelationshipSchema
                .builder()
                .addRelationshipType(relationshipType)
                .build();
        }
    }

    static RelationshipSchema of(Map<RelationshipType, Map<String, ValueType>> properties) {
        return RelationshipSchema.builder().properties(properties).build();
    }

    static Builder builder() {
        return new Builder().properties(new LinkedHashMap<>());
    }

    @AccessibleFields
    class Builder extends ImmutableRelationshipSchema.Builder {

        public Builder addProperty(
            RelationshipType type,
            String propertyName,
            ValueType relationshipProperty
        ) {
            this.properties
                .computeIfAbsent(type, ignore -> new LinkedHashMap<>())
                .put(propertyName, relationshipProperty);
            return this;
        }

        public Builder addRelationshipType(RelationshipType type) {
            this.properties.putIfAbsent(type, Collections.emptyMap());
            return this;
        }
    }
}
