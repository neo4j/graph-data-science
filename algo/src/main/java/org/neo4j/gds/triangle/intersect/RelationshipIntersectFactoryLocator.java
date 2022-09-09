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
package org.neo4j.gds.triangle.intersect;

import org.neo4j.gds.api.Graph;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public final class RelationshipIntersectFactoryLocator {

    private static final List<RelationshipIntersectFactory> FACTORIES;

    static {
        FACTORIES = ServiceLoader
            .load(RelationshipIntersectFactory.class, RelationshipIntersectFactory.class.getClassLoader())
            .stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toList());
    }

    public static Optional<RelationshipIntersectFactory> lookup(Graph graph) {
        return FACTORIES
            .stream()
            .filter(f -> f.canLoad(graph))
            .findFirst();
    }

    private RelationshipIntersectFactoryLocator() {}

}
