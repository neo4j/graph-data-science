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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class UserLogServicesTest {
    @Test
    void shouldScopeStoresToDatabase() {
        var service = new UserLogServices();

        var store1 = service.getUserLogStore(DatabaseId.of("some database"));
        var store2 = service.getUserLogStore(DatabaseId.of("some database"));
        var store3 = service.getUserLogStore(DatabaseId.of("some other database"));

        assertSame(store1, store2);
        assertNotSame(store1, store3);
    }

    @Test
    void shouldScopeFactoriesToDatabaseAndUser() {
        var service = new UserLogServices();

        var databaseId1 = DatabaseId.of("some database");
        var databaseId2 = DatabaseId.of("some other database");
        var username1 = new User("some user", false);
        var username2 = new User("some other user", false);
        var registry1 = service.getUserLogRegistry(databaseId1, username1);
        var registry2 = service.getUserLogRegistry(databaseId1, username1);
        var registry3 = service.getUserLogRegistry(databaseId2, username1);
        var registry4 = service.getUserLogRegistry(databaseId1, username2);

        assertSame(registry1, registry2);
        assertNotSame(registry1, registry3);
        assertNotSame(registry1, registry4);
        assertNotSame(registry3, registry4); // duh
    }

    @Test
    void shouldNotCareAboutUserAdminStatus() {
        var service = new UserLogServices();

        var databaseId = DatabaseId.of("some database");
        var username1 = new User("some user", false);
        var username2 = new User("some user", true);
        var registry1 = service.getUserLogRegistry(databaseId, username1);
        var registry2 = service.getUserLogRegistry(databaseId, username2);

        assertSame(registry1, registry2);
    }
}
