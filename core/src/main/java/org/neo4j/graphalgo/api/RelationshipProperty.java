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
package org.neo4j.graphalgo.api;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.RelationshipPropertySchema;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.values.storable.NumberType;

@ValueClass
public interface RelationshipProperty {

    Relationships.Properties values();

    RelationshipPropertySchema propertySchema();

    @Configuration.Ignore
    default String key() {
        return propertySchema().key();
    }

    @Configuration.Ignore
    default ValueType valueType() {
        return propertySchema().valueType();
    }

    @Configuration.Ignore
    default DefaultValue defaultValue() {
        return propertySchema().defaultValue();
    }

    @Configuration.Ignore
    default GraphStore.PropertyState propertyState() {
        return propertySchema().state();
    }

    @Configuration.Ignore
    default Aggregation aggregation() {
        return propertySchema().aggregation();
    }

    static RelationshipProperty of(
        String key,
        NumberType type,
        GraphStore.PropertyState state,
        Relationships.Properties values,
        DefaultValue defaultValue,
        Aggregation aggregation
    ) {
        return ImmutableRelationshipProperty.of(
            values,
            RelationshipPropertySchema.of(key, ValueType.fromNumberType(type), defaultValue, state, aggregation)
        );
    }
}
