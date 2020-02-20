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
package org.neo4j.graphalgo.compat;

import java.util.Map;

import org.neo4j.graphalgo.annotation.SuppressForbidden;

/**
 * Compatibility class for {@link org.neo4j.internal.helpers.collection.MapUtil}.
 * By using this class we reduce the number of references to the Neo4j helper class,
 * which means we break less when the Neo4j helper class moves between Neo4j versions.
 */
public final class MapUtil {

    private MapUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     * Delegates to {@link org.neo4j.internal.helpers.collection.MapUtil#map(Object...)} and allows static import.
     */
    @SuppressForbidden(reason = "Only allowed usage of org.neo4j.internal.helpers.collection.MapUtil")
    public static Map<String, Object> map(Object... objects) {
        return org.neo4j.internal.helpers.collection.MapUtil.map(objects);
    }

    /**
     * Delegates to {@link org.neo4j.internal.helpers.collection.MapUtil#map(Map, Object...)} and allows static import.
     */
    @SuppressForbidden(reason = "Only allowed usage of org.neo4j.internal.helpers.collection.MapUtil")
    public static Map<String, Object> map(Map<String, Object> targetMap, Object... objects) {
        return org.neo4j.internal.helpers.collection.MapUtil.map(targetMap, objects);
    }

    /**
     * Delegates to {@link org.neo4j.internal.helpers.collection.MapUtil#genericMap(Object...)} and allows static import.
     */
    @SuppressForbidden(reason = "Only allowed usage of org.neo4j.internal.helpers.collection.MapUtil")
    public static <K, V> Map<K, V> genericMap(Object... objects) {
        return org.neo4j.internal.helpers.collection.MapUtil.genericMap(objects);
    }

    /**
     * Delegates to {@link org.neo4j.internal.helpers.collection.MapUtil#genericMap(Map, Object...)} and allows static import.
     */
    @SuppressForbidden(reason = "Only allowed usage of org.neo4j.internal.helpers.collection.MapUtil")
    public static <K, V> Map<K, V> genericMap(Map<K, V> targetMap, Object... objects) {
        return org.neo4j.internal.helpers.collection.MapUtil.genericMap(targetMap, objects);
    }
}
