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

import org.neo4j.gds.Aggregation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Locale;

public record RelationshipPropertySchema(
    String key,
    ValueType valueType,
    DefaultValue defaultValue,
    PropertyState state,
    Aggregation aggregation
) implements PropertySchema {
    public RelationshipPropertySchema {
        aggregation = Aggregation.resolve(aggregation);
    }

    public String render() {
        return String.format(
            Locale.ENGLISH,
            "%s (%s, %s, Aggregation.%s)",
            valueType().cypherName(),
            defaultValue(),
            state(),
            aggregation());
    }

    public static RelationshipPropertySchema of(String propertyKey, ValueType valueType) {
        return RelationshipPropertySchema.of(
            propertyKey,
            valueType,
            valueType.fallbackValue(),
            PropertyState.PERSISTENT,
            Aggregation.NONE
        );
    }

    public static RelationshipPropertySchema of(String propertyKey, ValueType valueType, DefaultValue defaultValue, Aggregation aggregation) {
        return RelationshipPropertySchema.of(
            propertyKey,
            valueType,
            defaultValue,
            PropertyState.PERSISTENT,
            aggregation
        );
    }

    public static RelationshipPropertySchema of(String propertyKey, ValueType valueType, Aggregation aggregation) {
        return RelationshipPropertySchema.of(
            propertyKey,
            valueType,
            valueType.fallbackValue(),
            PropertyState.PERSISTENT,
            aggregation
        );
    }

    public static RelationshipPropertySchema of(
        String propertyKey,
        ValueType valueType,
        DefaultValue defaultValue,
        PropertyState propertyState,
        Aggregation aggregation
    ) {
        return new RelationshipPropertySchema(propertyKey, valueType, defaultValue, propertyState, aggregation);
    }
}
