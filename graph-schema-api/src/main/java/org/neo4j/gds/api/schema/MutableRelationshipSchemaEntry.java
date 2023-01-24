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
import org.neo4j.gds.core.Aggregation;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MutableRelationshipSchemaEntry implements RelationshipSchemaEntry {

    private final RelationshipType relationshipType;
    private final Direction direction;
    private final Map<String, RelationshipPropertySchema> properties;

    MutableRelationshipSchemaEntry(RelationshipType identifier, Direction direction) {
        this(identifier, direction, new LinkedHashMap<>());
    }

    public MutableRelationshipSchemaEntry(
        RelationshipType relationshipType,
        Direction direction,
        Map<String, RelationshipPropertySchema> properties
    ) {
        this.relationshipType = relationshipType;
        this.direction = direction;
        this.properties = properties;
    }

    static MutableRelationshipSchemaEntry from(RelationshipSchemaEntry fromEntry) {
        return new MutableRelationshipSchemaEntry(
            fromEntry.identifier(),
            fromEntry.direction(),
            new HashMap<>(fromEntry.properties())
        );
    }

    @Override
    public Direction direction() {
        return this.direction;
    }

    @Override
    public boolean isUndirected() {
        return direction() == Direction.UNDIRECTED;
    }

    @Override
    public RelationshipType identifier() {
        return relationshipType;
    }

    @Override
    public Map<String, RelationshipPropertySchema> properties() {
        return Map.copyOf(properties);
    }

    @Override
    public MutableRelationshipSchemaEntry union(MutableRelationshipSchemaEntry other) {
        if (!other.identifier().equals(this.identifier())) {
            throw new UnsupportedOperationException(
                formatWithLocale(
                    "Cannot union relationship schema entries with different types %s and %s",
                    this.identifier(),
                    other.identifier()
                )
            );
        }

        if (other.isUndirected() != this.isUndirected()) {
            throw new IllegalArgumentException(
                formatWithLocale("Conflicting directionality for relationship types %s", this.identifier().name));
        }

        return new MutableRelationshipSchemaEntry(this.identifier(), this.direction, unionProperties(other.properties));
    }

    @Override
    public Map<String, Object> toMap() {
        return Map.of(
            "direction", direction.name(),
            "properties", properties
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        innerEntry -> GraphSchema.forPropertySchema(innerEntry.getValue())
                    )
                )
        );
    }

    public MutableRelationshipSchemaEntry addProperty(String propertyKey, ValueType valueType, PropertyState propertyState) {
        return addProperty(
            propertyKey,
            RelationshipPropertySchema.of(
                propertyKey,
                valueType,
                valueType.fallbackValue(),
                propertyState,
                Aggregation.DEFAULT
            )
        );
    }

    public MutableRelationshipSchemaEntry addProperty(String propertyKey, RelationshipPropertySchema propertySchema) {
        this.properties.put(propertyKey, propertySchema);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutableRelationshipSchemaEntry that = (MutableRelationshipSchemaEntry) o;

        if (!relationshipType.equals(that.relationshipType)) return false;
        if (direction != that.direction) return false;
        return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        int result = relationshipType.hashCode();
        result = 31 * result + direction.hashCode();
        result = 31 * result + properties.hashCode();
        return result;
    }
}
