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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * While we strangle usages of context-injected {@link org.neo4j.gds.core.utils.progress.TaskStore}s,
 * this plays the role of supplying those.
 *
 * @deprecated This exists only until we stop using context-injected {@link org.neo4j.gds.core.utils.progress.TaskStore}s.
 */
@Deprecated
public class TaskStoreProvider implements ThrowingFunction<Context, TaskStore, ProcedureException> {
    private final DatabaseIdService databaseIdService;
    private final TaskStoreService taskStoreService;

    public TaskStoreProvider(DatabaseIdService databaseIdService, TaskStoreService taskStoreService) {
        this.databaseIdService = databaseIdService;
        this.taskStoreService = taskStoreService;
    }

    @Override
    public TaskStore apply(Context context) {
        DatabaseId databaseId = databaseIdService.getDatabaseId(context.graphDatabaseAPI());

        return taskStoreService.getTaskStore(databaseId);
    }
}
