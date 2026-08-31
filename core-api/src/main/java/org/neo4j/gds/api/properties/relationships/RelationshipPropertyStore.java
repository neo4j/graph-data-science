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
package org.neo4j.gds.api.properties.relationships;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public record RelationshipPropertyStore(Map<String, RelationshipProperty> relationshipProperties) {

    public RelationshipPropertyStore(String key, RelationshipProperty relationshipProperty) {
        this(Collections.singletonMap(key, relationshipProperty));
    }

    public boolean isEmpty() {
        return relationshipProperties().isEmpty();
    }

    public RelationshipProperty get(String propertyKey) {
        return relationshipProperties().get(propertyKey);
    }

    public RelationshipPropertyStore filter(String propertyKey) {
        return new RelationshipPropertyStore(propertyKey, get(propertyKey));
    }

    public Set<String> keySet() {
        return relationshipProperties().keySet();
    }

    public Collection<RelationshipProperty> values() {
        return relationshipProperties().values();
    }

    public boolean containsKey(String propertyKey) {
        return relationshipProperties().containsKey(propertyKey);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        Map<String, RelationshipProperty> builderRelationshipProperties;

        public Builder() {
            this.builderRelationshipProperties = new LinkedHashMap<>();
        }

        public Builder putIfAbsent(String propertyKey, RelationshipProperty relationshipProperty) {
            this.builderRelationshipProperties.putIfAbsent(propertyKey, relationshipProperty);
            return this;
        }

        public Builder putRelationshipProperty(String propertyKey, RelationshipProperty relationshipProperty) {
            this.builderRelationshipProperties.put(propertyKey, relationshipProperty);
            return this;
        }

        public RelationshipPropertyStore build() {
            return new RelationshipPropertyStore(this.builderRelationshipProperties);
        }
    }
}
