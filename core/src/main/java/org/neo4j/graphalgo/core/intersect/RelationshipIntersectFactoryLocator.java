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
package org.neo4j.graphalgo.core.intersect;

import org.neo4j.graphalgo.api.Graph;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class RelationshipIntersectFactoryLocator {

    private static final Map<Class<? extends Graph>, RelationshipIntersectFactory<? extends Graph>> CACHE = new ConcurrentHashMap<>();

    public static <G extends Graph> void register(Class<G> clazz, RelationshipIntersectFactory<G> fn) {
        CACHE.put(clazz, fn);
    }

    @SuppressWarnings("unchecked")
    public static Optional<RelationshipIntersectFactory<Graph>> lookup(Class<? extends Graph> clazz) {
        if (CACHE.containsKey(clazz)) {
            return Optional.of((RelationshipIntersectFactory<Graph>) CACHE.get(clazz));
        } else {
            return CACHE
                .keySet()
                .stream()
                .filter(keyClass -> keyClass.isAssignableFrom(clazz))
                .findFirst()
                .map(c -> (RelationshipIntersectFactory<Graph>) CACHE.get(c));
        }
    }

    private RelationshipIntersectFactoryLocator() {}

}
