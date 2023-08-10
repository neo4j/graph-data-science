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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.catalog.TaskRegistryFactoryService;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * @deprecated Needed until we strangle the last context-injected usages of {@link org.neo4j.gds.core.utils.progress.TaskRegistryFactory}
 */
@Deprecated
public class TaskRegistryFactoryProvider implements ThrowingFunction<Context, TaskRegistryFactory, ProcedureException> {
    private final DatabaseIdService databaseIdService;
    private final UserServices userServices;
    private final TaskRegistryFactoryService taskRegistryFactoryService;

    public TaskRegistryFactoryProvider(
        DatabaseIdService databaseIdService,
        UserServices userServices,
        TaskRegistryFactoryService taskRegistryFactoryService
    ) {
        this.databaseIdService = databaseIdService;
        this.userServices = userServices;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
    }

    @Override
    public TaskRegistryFactory apply(Context context) {
        var databaseId = databaseIdService.getDatabaseId(context.graphDatabaseAPI());
        var user = userServices.getUser(context.securityContext());

        return taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
    }
}
