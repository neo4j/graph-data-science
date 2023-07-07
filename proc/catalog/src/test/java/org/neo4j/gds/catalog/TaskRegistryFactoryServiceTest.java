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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskRegistryFactoryServiceTest {
    @Test
    void shouldHandOutDummiesWhenProgressTrackingEnabled() {
        var service = new TaskRegistryFactoryService(false, null);

        var databaseId = DatabaseId.from("some database");
        var user = new User("some user", false);
        var taskRegistryFactory = service.getTaskRegistryFactory(databaseId, user);

        assertEquals(TaskRegistryFactory.empty(), taskRegistryFactory);
    }

    @Test
    void shouldCreateNewFactoriesWhenNeeded() {
        var taskStoreService = mock(TaskStoreService.class);
        var service = new TaskRegistryFactoryService(true, taskStoreService);

        var databaseId = DatabaseId.from("some database");
        var user = new User("some user", false);
        when(taskStoreService.getTaskStore(databaseId)).thenReturn(mock(TaskStore.class));
        var factory = service.getTaskRegistryFactory(databaseId, user);

        assertNotNull(factory);
    }

    @Test
    void shouldEnsureFactoriesAreUniqueToDatabaseAndUser() {
        var taskStoreService = mock(TaskStoreService.class);
        var service = new TaskRegistryFactoryService(true, taskStoreService);

        var databaseId1 = DatabaseId.from("some database");
        var databaseId2 = DatabaseId.from("some other database");
        var user1 = new User("some user", false);
        var user2 = new User("some other user", false);
        when(taskStoreService.getTaskStore(databaseId1)).thenReturn(mock(TaskStore.class));
        when(taskStoreService.getTaskStore(databaseId2)).thenReturn(mock(TaskStore.class));
        var factory1 = service.getTaskRegistryFactory(databaseId1, user1);
        var factory2 = service.getTaskRegistryFactory(databaseId1, user1);

        /*
         * This is a little convoluted, but: you can logic it out if you squint
         */
        assertEquals(factory1, factory2);

        var factory3 = service.getTaskRegistryFactory(databaseId2, user1);

        assertNotEquals(factory1, factory3);

        var factory4 = service.getTaskRegistryFactory(databaseId2, user2);

        assertNotEquals(factory1, factory4);
        assertNotEquals(factory3, factory4);
    }
}
