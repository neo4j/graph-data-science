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
package org.neo4j.gds;

import org.neo4j.gds.PropertyMapping.Key;
import org.neo4j.gds.api.DefaultValue;

public final class PropertyMappingHelper {
    private PropertyMappingHelper() {}

    public static PropertyMapping of(String propertyKey) {
        return PropertyMapping.of(Key.simple(propertyKey));
    }
    public static PropertyMapping of(String propertyKey, Aggregation aggregation) {
        return PropertyMapping.of(Key.simple(propertyKey), aggregation);
    }
    public static PropertyMapping of(String propertyKey, long defaultValue) {
        return PropertyMapping.of(Key.simple(propertyKey), DefaultValue.of(defaultValue));
    }
    public static PropertyMapping of(String propertyKey, double defaultValue) {
        return PropertyMapping.of(Key.simple(propertyKey), DefaultValue.of(defaultValue));
    }
    public static PropertyMapping of(String internalKey, String externalKey, double defaultValue) {
        return PropertyMapping.of(Key.of(internalKey, externalKey), DefaultValue.of(defaultValue));
    }
    public static PropertyMapping of(String propertyKey, double[] defaultValue) {
        return PropertyMapping.of(Key.simple(propertyKey), DefaultValue.of(defaultValue));
    }
    public static PropertyMapping of(String propertyKey, long[] defaultValue) {
        return PropertyMapping.of(Key.simple(propertyKey), DefaultValue.of(defaultValue));
    }

}
