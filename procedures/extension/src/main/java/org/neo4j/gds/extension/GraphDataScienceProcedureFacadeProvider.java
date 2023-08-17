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
package org.neo4j.gds.extension;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.ProcedureCallContextReturnColumns;
import org.neo4j.gds.applications.graphstorecatalog.ConfigurationService;
import org.neo4j.gds.applications.graphstorecatalog.CypherProjectService;
import org.neo4j.gds.applications.graphstorecatalog.DefaultGraphStoreCatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.DropGraphService;
import org.neo4j.gds.applications.graphstorecatalog.DropNodePropertiesService;
import org.neo4j.gds.applications.graphstorecatalog.DropRelationshipsService;
import org.neo4j.gds.applications.graphstorecatalog.GenericProjectService;
import org.neo4j.gds.applications.graphstorecatalog.GraphMemoryUsageService;
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphProjectMemoryUsage;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreCatalogBusinessFacadePreConditionsDecorator;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreValidationService;
import org.neo4j.gds.applications.graphstorecatalog.ListGraphService;
import org.neo4j.gds.applications.graphstorecatalog.NativeProjectService;
import org.neo4j.gds.applications.graphstorecatalog.NodeLabelMutatorService;
import org.neo4j.gds.applications.graphstorecatalog.PreconditionsService;
import org.neo4j.gds.applications.graphstorecatalog.StreamNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.SubGraphProjectService;
import org.neo4j.gds.beta.filter.GraphStoreFilterService;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.executor.Preconditions;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.GraphDataScienceProcedureFacade;
import org.neo4j.gds.procedures.KernelTransactionService;
import org.neo4j.gds.procedures.ProcedureTransactionService;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.TransactionContextService;
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

public class GraphDataScienceProcedureFacadeProvider implements ThrowingFunction<Context, GraphDataScienceProcedureFacade, ProcedureException> {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final DatabaseIdService databaseIdService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final UserServices userServices;

    GraphDataScienceProcedureFacadeProvider(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseIdService databaseIdService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        UserServices userServices
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.databaseIdService = databaseIdService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.userServices = userServices;
    }

    @Override
    public GraphDataScienceProcedureFacade apply(Context context) throws ProcedureException {
        var graphStoreCatalogProcedureFacade = constructCatalogFacade(context);

        return new GraphDataScienceProcedureFacade(log, graphStoreCatalogProcedureFacade);
    }

    private CatalogFacade constructCatalogFacade(Context context) {
        // Neo4j's services, all encapsulated so that they can be resolved late
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransactionService = new KernelTransactionService(context);
        var procedureTransactionService = new ProcedureTransactionService(context);
        var procedureReturnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var terminationFlagService = new TerminationFlagService();
        var transactionContextService = new TransactionContextService();

        // GDS services
        var configurationService = new ConfigurationService();
        var graphStoreFilterService = new GraphStoreFilterService();
        var graphStoreValidationService = new GraphStoreValidationService();

        // GDS applications
        var dropGraphService = new DropGraphService(graphStoreCatalogService);
        var graphNameValidationService = new GraphNameValidationService();
        var listGraphService = new ListGraphService(graphStoreCatalogService);
        var graphProjectMemoryUsage = new GraphProjectMemoryUsage(log, graphDatabaseService);
        var nativeProjectService = new NativeProjectService(
            new GenericProjectService<>(
                log,
                graphDatabaseService,
                graphStoreCatalogService,
                graphProjectMemoryUsage,
                GraphProjectNativeResult.Builder::new
            ), graphProjectMemoryUsage
        );
        var cypherProjectService = new CypherProjectService(
            new GenericProjectService<>(
                log,
                graphDatabaseService,
                graphStoreCatalogService,
                graphProjectMemoryUsage,
                GraphProjectCypherResult.Builder::new
            ), graphProjectMemoryUsage
        );
        var subGraphProjectService = new SubGraphProjectService(log, graphStoreFilterService, graphStoreCatalogService);
        var graphMemoryUsageService = new GraphMemoryUsageService(graphStoreCatalogService);
        var dropNodePropertiesService = new DropNodePropertiesService(log);
        var dropRelationshipsService = new DropRelationshipsService(log);
        var nodeLabelMutatorService = new NodeLabelMutatorService();
        var streamNodePropertiesApplication = new StreamNodePropertiesApplication(log);

        // GDS business facade
        GraphStoreCatalogBusinessFacade businessFacade = new DefaultGraphStoreCatalogBusinessFacade(
            log,
            configurationService,
            graphNameValidationService,
            graphStoreCatalogService,
            graphStoreValidationService,
            dropGraphService,
            listGraphService,
            nativeProjectService,
            cypherProjectService,
            subGraphProjectService,
            graphMemoryUsageService,
            dropNodePropertiesService,
            dropRelationshipsService,
            nodeLabelMutatorService,
            streamNodePropertiesApplication
        );

        // wrap in decorator to enable preconditions checks
        var preconditionsService = createPreconditionsService();
        businessFacade = new GraphStoreCatalogBusinessFacadePreConditionsDecorator(
            businessFacade,
            preconditionsService
        );

        return new CatalogFacade(
            databaseIdService,
            graphDatabaseService,
            kernelTransactionService,
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
