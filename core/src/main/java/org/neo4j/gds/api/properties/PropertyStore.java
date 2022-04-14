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
package org.neo4j.gds.api.properties;

import org.immutables.value.Value;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface PropertyStore<VALUE extends PropertyValues, PROPERTY extends Property<VALUE>> {

    Map<String, PROPERTY> properties();

    default Map<String, VALUE> propertyValues() {
        return properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().values()));
    }

    default PROPERTY get(String propertyKey) {
        return properties().get(propertyKey);
    }

    default boolean isEmpty() {
        return properties().isEmpty();
    }

    @Value.Derived
    default Set<String> keySet() {
        return Collections.unmodifiableSet(properties().keySet());
    }

    default boolean containsKey(String propertyKey) {
        return properties().containsKey(propertyKey);
    }
}
