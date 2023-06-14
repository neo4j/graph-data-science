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
package org.neo4j.gds.procedure.facade;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.catalog.DatabaseIdService;
import org.neo4j.gds.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.gds.catalog.KernelTransactionService;
import org.neo4j.gds.catalog.ProcedureTransactionService;
import org.neo4j.gds.catalog.TaskRegistryFactoryService;
import org.neo4j.gds.catalog.UserLogServices;
import org.neo4j.gds.catalog.UsernameService;
import org.neo4j.gds.core.loading.GraphNameValidationService;
import org.neo4j.gds.core.loading.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PreconditionsService;
import org.neo4j.gds.executor.Preconditions;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.gds.logging.Log;

/**
 * So here we set up the entire application with all it's services.
 * We have to inject the context, the stuff we get in plugin init from Neo4j.
 * And then we can construct the service graph for our application.
 */
public class ProcedureFacadeProvider implements ThrowingFunction<Context, GraphStoreCatalogProcedureFacade, ProcedureException> {
    private final Log log;
    private final DatabaseIdService databaseIdService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final UsernameService usernameService;

    ProcedureFacadeProvider(
        Log log,
        DatabaseIdService databaseIdService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        UsernameService usernameService
    ) {
        this.log = log;
        this.databaseIdService = databaseIdService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.usernameService = usernameService;
    }

    /**
     * The application is assembled here. Manage it well or complexity explodes.
     * This facade _is_ the application, the procedure stubs are just some dumb UI
     */
    @Override
    public GraphStoreCatalogProcedureFacade apply(Context context) {
        // Neo4j's services, all encapsulated so that they can be resolved late
        var kernelTransactionService = new KernelTransactionService(context);
        var procedureTransactionService = new ProcedureTransactionService(context);

        // GDS Business Facade
        var preconditionsService = createPreconditionsService();
        var businessFacade = new GraphStoreCatalogBusinessFacade(
            preconditionsService,
            new GraphNameValidationService(),
            new GraphStoreCatalogService()
        );

        return new GraphStoreCatalogProcedureFacade(
            databaseIdService,
            context.graphDatabaseAPI(),
            kernelTransactionService,
            log,
            procedureTransactionService,
            context.securityContext(),
            taskRegistryFactoryService,
            userLogServices,
            usernameService,
            businessFacade
        );
    }

    private static PreconditionsService createPreconditionsService() {
        return Preconditions::check;
    }
}
