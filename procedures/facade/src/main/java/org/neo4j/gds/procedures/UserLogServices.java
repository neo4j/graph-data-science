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

import org.neo4j.gds.user.log.PerDatabaseUserLogStore;
import org.neo4j.gds.user.log.UserLogRegistry;
import org.neo4j.gds.user.log.UserLogStore;

/**
 * We have a user log store per database, and registries per database and user.
 * This allows us to easily do things like getting user log entries for just the database they are working on,
 * or all log entries pertaining to a database.
 */
public class UserLogServices {
    public UserLogStore getUserLogStore() {
        return new PerDatabaseUserLogStore();
    }

    public UserLogRegistry getUserLogRegistry() {
        return new UserLogRegistry();
    }
}
