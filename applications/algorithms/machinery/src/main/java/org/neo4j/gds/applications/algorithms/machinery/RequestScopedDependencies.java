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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.annotation.GenerateBuilder;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogStore;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * This is a handy class for transporting similarly scoped dependencies through layers.
 * And especially useful when that list grows or shrinks - fewer sites to edit innit.
 */
@GenerateBuilder
public record RequestScopedDependencies(
    DatabaseId databaseId,
    GraphLoaderContext graphLoaderContext,
    TaskRegistryFactory taskRegistryFactory,
    TaskStore taskStore,
    TerminationFlag terminationFlag,
    User user,
    UserLogRegistryFactory userLogRegistryFactory,
    UserLogStore userLogStore
) {
    public static RequestScopedDependenciesBuilder builder() {
        return RequestScopedDependenciesBuilder.builder();
    }
}
