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
package org.neo4j.gds.api;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;

import java.util.Optional;

@ValueClass
public interface DatabaseInfo {

    static DatabaseInfo of(DatabaseId databaseId, DatabaseLocation databaseLocation) {
        return ImmutableDatabaseInfo.of(databaseId, databaseLocation, Optional.empty());
    }

    DatabaseId databaseId();

    DatabaseLocation databaseLocation();

    Optional<DatabaseId> remoteDatabaseId();

    @Value.Check()
    default void validateRemoteDatabaseHasIdSet() {
        if (databaseLocation() == DatabaseLocation.REMOTE) {
            if (remoteDatabaseId().isEmpty()) {
                throw new IllegalStateException("Remote database id must be set when database location is remote");
            }
        } else {
            if (remoteDatabaseId().isPresent()) {
                throw new IllegalStateException("Remote database id must not be set when database location is not remote");
            }
        }
    }

    enum DatabaseLocation {
        LOCAL, REMOTE, NONE;
    }
}
