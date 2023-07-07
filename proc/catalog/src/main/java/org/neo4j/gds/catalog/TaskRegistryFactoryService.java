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

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.progress.LocalTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStoreService;

/**
 * This should be the one-stop-shop for {@link org.neo4j.gds.core.utils.progress.TaskRegistryFactory}s.
 * Therefore, it must be s application-wide singleton, created exactly once in the Procedure Facade,
 * and it lives for the lifetime of the plugin.
 * Factories are unique to a database and user as that allows us neat reporting,
 * like the running jobs for this user for this database.
 * In turn these factories rely on database-scoped {@link org.neo4j.gds.core.utils.progress.TaskStore}s.
 */
public class TaskRegistryFactoryService {
    private final boolean progressTrackingEnabled;
    private final TaskStoreService taskStoreService;

    public TaskRegistryFactoryService(boolean progressTrackingEnabled, TaskStoreService taskStoreService) {
        this.taskStoreService = taskStoreService;
        this.progressTrackingEnabled = progressTrackingEnabled;
    }

    /**
     * The task _store_ is shared for the database; the _registry_ is newed up per request,
     * because it is dependent on the current user for the request.
     */
    public TaskRegistryFactory getTaskRegistryFactory(DatabaseId databaseId, User user) {
        if (!progressTrackingEnabled) return TaskRegistryFactory.empty();

        var taskStoreForDatabase = taskStoreService.getTaskStore(databaseId);

        return new LocalTaskRegistryFactory(user.getUsername(), taskStoreForDatabase);
    }
}
