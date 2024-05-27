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

import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.graphstorecatalog.GraphProjectMemoryUsageService;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.catalog.CatalogProcedureFacade;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.function.Consumer;

/**
 * Here we keep everything related to constructing the {@link org.neo4j.gds.procedures.catalog.CatalogProcedureFacade}
 * from a {@link org.neo4j.kernel.api.procedure.Context}, at request time.
 * <p>
 * We can resolve things like user and database id here, construct termination flags, and such.
 */
public class CatalogProcedureFacadeFactory {
    // dull bits
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final TerminationFlagAccessor terminationFlagAccessor = new TerminationFlagAccessor();
    private final TransactionContextAccessor transactionContextAccessor = new TransactionContextAccessor();
    private final UserAccessor userAccessor = new UserAccessor();

    // Global scoped/ global state/ stateless things
    private final Log log;

    // Request scoped things
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;

    /**
     * We inject services here so that we may control and isolate access to dependencies.
     * Take {@link org.neo4j.gds.procedures.UserAccessor} for example.
     * Without it, I would have to stub out Neo4j's {@link org.neo4j.kernel.api.procedure.Context}, in a non-trivial,
     * ugly way. Now instead I can inject the user by stubbing out GDS' own little POJO service.
     */
    public CatalogProcedureFacadeFactory(
        Log log,
        ExporterBuildersProviderService exporterBuildersProviderService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices
    ) {
        this.log = log;

        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
    }

    /**
     * We construct the catalog facade at request time. At this point things like user and database id are set in stone.
     * And we can readily construct things like termination flags.
     */
    CatalogProcedureFacade createCatalogProcedureFacade(
        ApplicationsFacade applicationsFacade,
        GraphDatabaseService graphDatabaseService,
        KernelTransaction kernelTransaction,
        Transaction procedureTransaction,
        ProcedureCallContext procedureCallContext,
        SecurityContext securityContext,
        ExporterContext exporterContext
    ) {
        // Derived data and services
        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);
        var graphProjectMemoryUsageService = new GraphProjectMemoryUsageService(log, graphDatabaseService);
        var procedureReturnColumns = new ProcedureCallContextReturnColumns(procedureCallContext);
        var streamCloser = new Consumer<AutoCloseable>() {
            @Override
            public void accept(AutoCloseable autoCloseable) {
                Neo4jProxy.registerCloseableResource(kernelTransaction, autoCloseable);
            }
        };
        var terminationFlag = terminationFlagAccessor.createTerminationFlag(kernelTransaction);
        var transactionContext = transactionContextAccessor.transactionContext(
            graphDatabaseService,
            procedureTransaction
        );
        var user = userAccessor.getUser(securityContext);
        var userLogStore = userLogServices.getUserLogStore(databaseId);

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        // Exporter builders
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var nodeLabelExporterBuilder = exportBuildersProvider.nodeLabelExporterBuilder(exporterContext);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipPropertiesExporterBuilder = exportBuildersProvider.relationshipPropertiesExporterBuilder(
            exporterContext);

        return new CatalogProcedureFacade(
            streamCloser,
            databaseId,
            graphDatabaseService,
            graphProjectMemoryUsageService,
            nodeLabelExporterBuilder,
            nodePropertyExporterBuilder,
            procedureReturnColumns,
            relationshipExporterBuilder,
            relationshipPropertiesExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            user,
            userLogRegistryFactory,
            userLogStore,
            applicationsFacade
        );
    }
}
