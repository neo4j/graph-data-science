/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

abstract class EntityFilter {

    static final String PROPERTIES_KEY = "properties";

    public final PropertyMappings properties;

    EntityFilter(PropertyMappings properties) {
        this.properties = properties;
    }

    public abstract boolean isEmpty();

    public final Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        writeToObject(value);
        value.put(PROPERTIES_KEY, properties.toObject());
        return value;
    }

    static <T extends EntityFilter> T create(
        Map<String, Object> config,
        Function<PropertyMappings, T> constructor
    ) {
        Object properties = config.getOrDefault(PROPERTIES_KEY, emptyMap());
        PropertyMappings propertyMappings = PropertyMappings.fromObject(properties);
        return constructor.apply(propertyMappings);
    }

    static String nonEmptyString(Map<String, Object> config, String key) {
        @Nullable Object value = config.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "'%s' is not a valid value for  the key '%s'",
                value, key
            ));
        }
        return (String) value;
    }

    abstract void writeToObject(Map<String, Object> value);
}
