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
package org.neo4j.gds.procedures;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.warnings.PerDatabaseUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.core.utils.warnings.UserLogStore;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We have a user log store per database, and registries per database and user.
 * This allows us to easily do things like getting user log entries for just the database they are working on,
 * or all log entries pertaining to a database.
 */
public class UserLogServices {
    private final Map<DatabaseId, UserLogStore> stores = new ConcurrentHashMap<>();
    private final Map<DatabaseId, Map<User, UserLogRegistry>> registries = new ConcurrentHashMap<>();

    public UserLogStore getUserLogStore(DatabaseId databaseId) {
        return stores.computeIfAbsent(databaseId, _ -> new PerDatabaseUserLogStore());
    }

    public UserLogRegistry getUserLogRegistry(DatabaseId databaseId, User user) {
        var registryForDatabase = getRegistryForDatabase(databaseId);

        return getRegistryForUser(databaseId, user, registryForDatabase);
    }

    private UserLogRegistry getRegistryForUser(
        DatabaseId databaseId,
        User user,
        Map<User, UserLogRegistry> registryByUser
    ) {
        return registryByUser.computeIfAbsent(
            user, u -> {
                var userLogStoreForDatabase = getUserLogStore(databaseId);

                return new UserLogRegistry(u, userLogStoreForDatabase);
            }
        );
    }

    private Map<User, UserLogRegistry> getRegistryForDatabase(DatabaseId databaseId) {
        return registries.computeIfAbsent(
            databaseId,
            _ -> new ConcurrentSkipListMap<>(Comparator.comparing(User::getUsername))
        );
    }
}
