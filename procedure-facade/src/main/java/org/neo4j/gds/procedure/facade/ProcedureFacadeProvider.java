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
import org.neo4j.gds.ProcedureCallContextReturnColumns;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.NativeProjectService;
import org.neo4j.gds.catalog.DatabaseIdService;
import org.neo4j.gds.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.gds.catalog.KernelTransactionService;
import org.neo4j.gds.catalog.ProcedureTransactionService;
import org.neo4j.gds.catalog.TaskRegistryFactoryService;
import org.neo4j.gds.catalog.TerminationFlagService;
import org.neo4j.gds.catalog.TransactionContextService;
import org.neo4j.gds.catalog.UserLogServices;
import org.neo4j.gds.catalog.UserServices;
import org.neo4j.gds.core.loading.ConfigurationService;
import org.neo4j.gds.applications.graphstorecatalog.DropGraphService;
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.ListGraphService;
import org.neo4j.gds.applications.graphstorecatalog.PreconditionsService;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.executor.Preconditions;
import org.neo4j.gds.logging.Log;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

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
    private final UserServices userServices;

    ProcedureFacadeProvider(
        Log log,
        DatabaseIdService databaseIdService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        UserServices userServices
    ) {
        this.log = log;
        this.databaseIdService = databaseIdService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.userServices = userServices;
    }

    /**
     * The application is assembled here. Manage it well or complexity explodes.
     * This facade _is_ the application, the procedure stubs are just some dumb UI
     */
    @Override
    public GraphStoreCatalogProcedureFacade apply(Context context) {
        // Neo4j's services, all encapsulated so that they can be resolved late
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransactionService = new KernelTransactionService(context);
        var procedureTransactionService = new ProcedureTransactionService(context);
        var procedureReturnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var terminationFlagService = new TerminationFlagService();
        var transactionContextService = new TransactionContextService();

        // GDS services
        var graphStoreCatalogService = new GraphStoreCatalogService();

        // GDS applications
        var configurationService = new ConfigurationService();
        var dropGraphService = new DropGraphService(graphStoreCatalogService);
        var graphNameValidationService = new GraphNameValidationService();
        var listGraphService = new ListGraphService(graphStoreCatalogService);
        var nativeProjectService = new NativeProjectService(log, graphDatabaseService);
        var preconditionsService = createPreconditionsService();

        // GDS business facade
        var businessFacade = new GraphStoreCatalogBusinessFacade(
            preconditionsService,
            configurationService,
            graphNameValidationService,
            graphStoreCatalogService,
            dropGraphService,
            listGraphService,
            nativeProjectService
        );

        return new GraphStoreCatalogProcedureFacade(
            databaseIdService,
            graphDatabaseService,
            kernelTransactionService,
            log,
            procedureReturnColumns,
            procedureTransactionService,
            context.securityContext(),
            taskRegistryFactoryService,
            terminationFlagService,
            transactionContextService,
            userLogServices,
            userServices,
            businessFacade
        );
    }

    private static PreconditionsService createPreconditionsService() {
        return Preconditions::check;
    }
}
