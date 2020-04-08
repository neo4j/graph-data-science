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
package org.neo4j.graphalgo.core.schema;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.values.storable.NumberType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@ValueClass
public interface RelationshipSchema {
    Map<RelationshipType, Map<String, NumberType>> properties();

    default Map<String, Object> toMap() {
        return properties().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().name,
            entry -> entry
                .getValue()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    innerEntry -> GraphStoreSchema.fromNumberType(innerEntry.getValue()))
                )
        ));
    }

    static RelationshipSchema of(Map<RelationshipType, Map<String, NumberType>> properties) {
        return RelationshipSchema.builder().properties(properties).build();
    }

    static Builder builder() {
        return new RelationshipSchema.Builder();
    }

    @org.immutables.builder.Builder.AccessibleFields
    class Builder extends ImmutableRelationshipSchema.Builder {

        public void addPropertyAndTypeForRelationshipType(String type, String propertyName, NumberType relationshipProperty) {
            if (this.properties == null) {
                this.properties = new LinkedHashMap<>();
            }
            RelationshipType relationshipType = toRelationshipType(type);
            this.properties
                .computeIfAbsent(relationshipType, ignore -> new LinkedHashMap<>())
                .put(propertyName, relationshipProperty);
        }

        public void addEmptyMapForRelationshipTypeWithoutProperties(String type) {
            RelationshipType relationshipType = toRelationshipType(type);
            if (this.properties == null) {
                this.putProperty(relationshipType, Collections.emptyMap());
            } else {
                this.properties.putIfAbsent(relationshipType, Collections.emptyMap());
            }
        }

        // TODO: remove if "" as a rel-type is impossible
        private RelationshipType toRelationshipType(String type) {
            return RelationshipType.of(type.isEmpty() ? "*" : type);
        }
    }
}
