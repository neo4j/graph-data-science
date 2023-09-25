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
package org.neo4j.gds.core.utils.progress;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @deprecated This needs to live here until we refactor the core -> progress tracking dependency.
 */
@Deprecated
class TaskStoreServiceTest {
    @Test
    void shouldRespectToggle() {
        var taskStoreService = new TaskStoreService(false);

        var taskStore = taskStoreService.getTaskStore(DatabaseId.of("a database"));

        assertEquals(EmptyTaskStore.INSTANCE, taskStore);
    }

    @Test
    void shouldGetOrCreatePerDatabase() {
        var taskStoreService = new TaskStoreService(true);

        var taskStore1 = taskStoreService.getTaskStore(DatabaseId.of("some database"));
        var taskStore2 = taskStoreService.getTaskStore(DatabaseId.of("some database"));
        var taskStore3 = taskStoreService.getTaskStore(DatabaseId.of("some other database"));

        assertSame(taskStore1, taskStore2);
        assertNotSame(taskStore1, taskStore3);
    }
}
