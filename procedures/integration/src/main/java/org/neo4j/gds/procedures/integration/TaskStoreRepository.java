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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.utils.progress.TaskStore;

import java.util.HashMap;
import java.util.Map;

/**
 * This should be a single instance per system.
 * It holds the state of all task stores across data sources in the system.
 * It is _not_ a singleton across the JVM, so you could set up multiple systems in a single JVM,
 * with each its own repository, and have them suitably isolated.
 */
public class TaskStoreRepository {
    private final Map<DatabaseId, TaskStore> taskStores = new HashMap<>();

    /**
     * You call this when bootstrapping for a data source
     */
    void add(DatabaseId databaseId, TaskStore taskStore) {
        taskStores.put(databaseId, taskStore);
    }

    /**
     * It is guaranteed that a task store exists for your data source; it is programmer error if not.
     */
    TaskStore get(DatabaseId databaseId) {
        return taskStores.get(databaseId);
    }

    /**
     * You call this when a data source goes away
     */
    void remove(DatabaseId databaseId) {
        taskStores.remove(databaseId);
    }
}
