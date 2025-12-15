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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.common.DependencyResolver;

import java.util.Optional;

/**
 * We need this to stand in when a dependency resolver is required, but not used. No, I don't agree with it either.
 */
class StandInDependencyResolver implements DependencyResolver {
    @Override
    public <T> Optional<T> resolveOptionalDependency(Class<T> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T resolveDependency(Class<T> type, SelectionStrategy selector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsDependency(Class<?> type) {
        throw new UnsupportedOperationException();
    }
}
