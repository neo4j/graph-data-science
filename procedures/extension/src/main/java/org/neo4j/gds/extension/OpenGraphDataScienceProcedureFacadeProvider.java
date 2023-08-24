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
import org.neo4j.gds.applications.graphstorecatalog.CypherProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.DefaultGraphStoreCatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.DropGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.DropNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.DropRelationshipsApplication;
import org.neo4j.gds.applications.graphstorecatalog.GenericProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphMemoryUsageApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphProjectMemoryUsageService;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreCatalogBusinessFacadePreConditionsDecorator;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreValidationService;
import org.neo4j.gds.applications.graphstorecatalog.ListGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.NativeProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.NodeLabelMutatorApplication;
import org.neo4j.gds.applications.graphstorecatalog.PreconditionsService;
import org.neo4j.gds.applications.graphstorecatalog.StreamNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamRelationshipPropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamRelationshipsApplication;
import org.neo4j.gds.applications.graphstorecatalog.SubGraphProjectApplication;
import org.neo4j.gds.beta.filter.GraphStoreFilterService;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.executor.Preconditions;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.OpenGraphDataScience;
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

public class OpenGraphDataScienceProcedureFacadeProvider implements ThrowingFunction<Context, OpenGraphDataScience, ProcedureException> {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final DatabaseIdService databaseIdService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final UserServices userServices;

    OpenGraphDataScienceProcedureFacadeProvider(
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
    public OpenGraphDataScience apply(Context context) throws ProcedureException {
        var graphStoreCatalogProcedureFacade = constructCatalogFacade(context);

        return new OpenGraphDataScience(log, graphStoreCatalogProcedureFacade);
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
        var graphNameValidationService = new GraphNameValidationService();
        var graphProjectMemoryUsage = new GraphProjectMemoryUsageService(log, graphDatabaseService);
        var graphStoreFilterService = new GraphStoreFilterService();
        var graphStoreValidationService = new GraphStoreValidationService();

        // GDS applications
        var dropGraphApplication = new DropGraphApplication(graphStoreCatalogService);
        var listGraphApplication = new ListGraphApplication(graphStoreCatalogService);
        var nativeProjectApplication = new NativeProjectApplication(
            new GenericProjectApplication<>(
                log,
                graphDatabaseService,
                graphStoreCatalogService,
                graphProjectMemoryUsage,
                GraphProjectNativeResult.Builder::new
            ), graphProjectMemoryUsage
        );
        var cypherProjectApplication = new CypherProjectApplication(
            new GenericProjectApplication<>(
                log,
                graphDatabaseService,
                graphStoreCatalogService,
                graphProjectMemoryUsage,
                GraphProjectCypherResult.Builder::new
            ), graphProjectMemoryUsage
        );
        var subGraphProjectApplication = new SubGraphProjectApplication(log, graphStoreFilterService, graphStoreCatalogService);
        var graphMemoryUsageApplication = new GraphMemoryUsageApplication(graphStoreCatalogService);
        var dropNodePropertiesApplication = new DropNodePropertiesApplication(log);
        var dropRelationshipsApplication = new DropRelationshipsApplication(log);
        var nodeLabelMutatorApplication = new NodeLabelMutatorApplication();
        var streamNodePropertiesApplication = new StreamNodePropertiesApplication(log);
        var streamRelationshipPropertiesApplication = new StreamRelationshipPropertiesApplication(log);
        var streamRelationshipsApplication = new StreamRelationshipsApplication();

        // GDS business facade
        GraphStoreCatalogBusinessFacade businessFacade = new DefaultGraphStoreCatalogBusinessFacade(
            log,
            configurationService,
            graphNameValidationService,
            graphStoreCatalogService,
            graphStoreValidationService,
            dropGraphApplication,
            listGraphApplication,
            nativeProjectApplication,
            cypherProjectApplication,
            subGraphProjectApplication,
            graphMemoryUsageApplication,
            dropNodePropertiesApplication,
            dropRelationshipsApplication,
            nodeLabelMutatorApplication,
            streamNodePropertiesApplication,
            streamRelationshipPropertiesApplication,
            streamRelationshipsApplication
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
