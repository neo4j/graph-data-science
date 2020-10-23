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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Optional;

@ValueClass
public
interface NodeProperty {

    String key();

    GraphStore.PropertyState state();

    NodeProperties values();

    Optional<DefaultValue> maybeDefaultValue();

    @Configuration.Ignore
    default ValueType type() {
        return values().valueType();
    };

    static NodeProperty of(
        String key,
        GraphStore.PropertyState origin,
        NodeProperties values
    ) {
        return ImmutableNodeProperty.of(key, origin, values, Optional.empty());
    }

    static NodeProperty of(
        String key,
        GraphStore.PropertyState origin,
        NodeProperties values,
        Optional<DefaultValue> maybeDefaultValue
    ) {
        return ImmutableNodeProperty.of(key, origin, values, maybeDefaultValue);
    }
}
