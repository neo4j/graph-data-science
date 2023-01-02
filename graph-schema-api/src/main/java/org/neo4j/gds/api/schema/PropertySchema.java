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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

@ValueClass
public interface PropertySchema {

    String key();

    ValueType valueType();

    DefaultValue defaultValue();

    @Value.Auxiliary
    PropertyState state();

    static PropertySchema of(String propertyKey, ValueType valueType) {
        return ImmutablePropertySchema.of(propertyKey, valueType, valueType.fallbackValue(), PropertyState.PERSISTENT);
    }

    static PropertySchema of(String propertyKey, ValueType valueType, DefaultValue defaultValue, PropertyState propertyState) {
        return ImmutablePropertySchema.of(propertyKey, valueType, defaultValue, propertyState);
    }
}
