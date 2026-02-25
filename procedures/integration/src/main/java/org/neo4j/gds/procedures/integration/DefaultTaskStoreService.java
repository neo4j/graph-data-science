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
import org.neo4j.gds.core.utils.progress.TaskStoreService;

/**
 * This is a general purpose task store service that you use to obtain task stores from data source ids.
 * It is backed by a repository that does the storing and mapping.
 */
public class DefaultTaskStoreService implements TaskStoreService {
    private final TaskStoreFactory taskStoreFactory;
    private final TaskStoreRepository taskStoreRepository;

    public DefaultTaskStoreService(TaskStoreFactory taskStoreFactory, TaskStoreRepository taskStoreRepository) {
        this.taskStoreFactory = taskStoreFactory;
        this.taskStoreRepository = taskStoreRepository;
    }

    @Override
    public TaskStore getOrCreateTaskStore(DatabaseId databaseId) {
        var taskStore = taskStoreRepository.get(databaseId);

        if (taskStore != null) return taskStore; // get

        taskStore = taskStoreFactory.create(); // create

        taskStoreRepository.add(databaseId, taskStore);

        return taskStore;
    }
}
