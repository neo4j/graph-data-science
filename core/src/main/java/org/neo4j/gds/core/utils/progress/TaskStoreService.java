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

import org.neo4j.gds.api.DatabaseId;

/**
 * This class should hold all {@link org.neo4j.gds.core.utils.progress.TaskStore}s for the application.
 * Therefore, it should be a singleton. You instantiate it up once as part of assembling the application.
 * TaskStores are tied to databases and live for the lifetime of a database.
 */
public class TaskStoreService {
    /**
     * This is a temporary hack where we allow Procedure Facade to control application state,
     * but also retain the old functionality of TaskStores being made available for context injection.
     * We do this so that we may slice our software vertically while migrating it;
     * this hack should go away when TaskStores are no longer needed for context injection.
     */
    // private final Map<DatabaseId, TaskStore> taskStores = new ConcurrentHashMap();

    private final boolean progressTrackingEnabled;

    public TaskStoreService(boolean progressTrackingEnabled) {
        this.progressTrackingEnabled = progressTrackingEnabled;
    }

    public TaskStore getTaskStore(DatabaseId databaseId) {
        if (!progressTrackingEnabled) return EmptyTaskStore.INSTANCE;

        return TaskStoreHolder.getTaskStore(databaseId.databaseName());
        // return taskStores.computeIfAbsent(databaseId.databaseName(), __ -> new PerDatabaseTaskStore());
    }
}
