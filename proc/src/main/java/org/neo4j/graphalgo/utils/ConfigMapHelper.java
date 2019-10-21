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

package org.neo4j.graphalgo.utils;

import java.util.Map;

public final class ConfigMapHelper {
    private ConfigMapHelper() {}

    public static String getString(Map<String, Object> map, String key, String defaultValue) {
        if (!map.containsKey(key)) {
            return defaultValue;
        } else {
            return getString(map, key);
        }
    }

    public static String getString(Map<String, Object> map, String key) {
        checkExistance(map, key);

        Object value = map.get(key);
        if (!(value instanceof String)) {
            throw generateTypeException(key, "String", value);
        }

        return (String) value;
    }

    public static double getDouble(Map<String, Object> map, String key, double defaultValue) {
        if (!map.containsKey(key)) {
            return defaultValue;
        } else {
            return getDouble(map, key);
        }
    }

    public static double getDouble(Map<String, Object> map, String key) {
        checkExistance(map, key);

        Object value = map.get(key);
        if (!(value instanceof Double)) {
            throw generateTypeException(key, "Double", value);
        }

        return (double) value;
    }

    private static void checkExistance(Map<String, Object> map, String key) {
        if (!map.containsKey(key)) {
            throw new IllegalArgumentException(String.format(
                    "Relationship property generator property `%s` needs to be specified",
                    key));
        }
    }

    private static IllegalArgumentException generateTypeException(String key, String expectedType, Object value) {
        String actualType = value == null ? "null" : value.getClass().getSimpleName();

        return new IllegalArgumentException(String.format(
                "Relationship property generator property `%s` needs to be of type `%s` but was `%s`",
                key,
                expectedType,
                actualType));
    }

}