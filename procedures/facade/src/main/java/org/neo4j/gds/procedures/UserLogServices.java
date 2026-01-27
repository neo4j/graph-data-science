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
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.core.utils.warnings.UserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogStoreHolder;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We have a user log store per database, and registry factories per database and user.
 * This allows us to easily do things like getting user log entries for just the database they are working on,
 * or all log entries pertaining to a database.
 */
public class UserLogServices {
    // private final Map<DatabaseId, UserLogStore> stores = new ConcurrentHashMap<>();
    private final Map<DatabaseId, Map<User, UserLogRegistry>> registries = new ConcurrentHashMap<>();

    public UserLogStore getUserLogStore(DatabaseId databaseId) {
        String databaseName = databaseId.databaseName();

        return UserLogStoreHolder.getUserLogStore(databaseName);
        // return stores.computeIfAbsent(databaseId, __ -> new BetterUserLogStore());
    }

    public UserLogRegistry getUserLogRegistry(DatabaseId databaseId, User user) {
        var registryByUser = getRegistryForDatabase(databaseId);

        return registryByUser.computeIfAbsent(user, u -> {
            var userLogStoreForDatabase = getUserLogStore(databaseId);

            return new UserLogRegistry(u.getUsername(), userLogStoreForDatabase);
        });
    }

    private Map<User, UserLogRegistry> getRegistryForDatabase(DatabaseId databaseId) {
        return registries.computeIfAbsent(
            databaseId,
            __ -> new ConcurrentSkipListMap<>(Comparator.comparing(User::getUsername))
        );
    }
}
