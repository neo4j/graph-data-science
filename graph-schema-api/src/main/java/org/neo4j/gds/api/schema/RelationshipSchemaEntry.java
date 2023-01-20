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

public class RelationshipSchemaEntry extends ElementSchemaEntry<RelationshipSchemaEntry, RelationshipType, RelationshipPropertySchema> {

    private final Direction direction;

    RelationshipSchemaEntry(RelationshipType identifier, Direction direction) {
        this(identifier, direction, new LinkedHashMap<>());
    }

    public RelationshipSchemaEntry(
        RelationshipType identifier,
        Direction direction,
        Map<String, RelationshipPropertySchema> properties
    ) {
        super(identifier, properties);
        this.direction = direction;
    }

    static RelationshipSchemaEntry from(RelationshipSchemaEntry fromEntry) {
        return new RelationshipSchemaEntry(
            fromEntry.identifier(),
            fromEntry.direction,
            new HashMap<>(fromEntry.properties())
        );
    }

    public Direction direction() {
        return this.direction;
    }

    boolean isUndirected() {
        return direction() == Direction.UNDIRECTED;
    }

    @Override
    public RelationshipSchemaEntry union(RelationshipSchemaEntry other) {
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

        return new RelationshipSchemaEntry(this.identifier(), this.direction, unionProperties(other.properties));
    }

    public RelationshipSchemaEntry addProperty(String propertyKey, ValueType valueType, PropertyState propertyState) {
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

    public RelationshipSchemaEntry addProperty(String propertyKey, RelationshipPropertySchema propertySchema) {
        this.properties.put(propertyKey, propertySchema);
        return this;
    }

    @Override
    Map<String, Object> toMap() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RelationshipSchemaEntry that = (RelationshipSchemaEntry) o;

        return direction == that.direction;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + direction.hashCode();
        return result;
    }
}
