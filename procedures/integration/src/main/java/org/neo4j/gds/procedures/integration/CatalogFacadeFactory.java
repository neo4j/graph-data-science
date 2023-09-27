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

import org.neo4j.gds.ProcedureCallContextReturnColumns;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.ConfigurationService;
import org.neo4j.gds.applications.graphstorecatalog.CypherProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.DefaultCatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.DropGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.DropNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.DropRelationshipsApplication;
import org.neo4j.gds.applications.graphstorecatalog.EstimateCommonNeighbourAwareRandomWalkApplication;
import org.neo4j.gds.applications.graphstorecatalog.GenerateGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.GenericProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphMemoryUsageApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphProjectMemoryUsageService;
import org.neo4j.gds.applications.graphstorecatalog.GraphSamplingApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreValidationService;
import org.neo4j.gds.applications.graphstorecatalog.ListGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.NativeProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.NodeLabelMutatorApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamRelationshipPropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamRelationshipsApplication;
import org.neo4j.gds.applications.graphstorecatalog.SubGraphProjectApplication;
import org.neo4j.gds.applications.graphstorecatalog.WriteNodeLabelApplication;
import org.neo4j.gds.applications.graphstorecatalog.WriteNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.WriteRelationshipPropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.WriteRelationshipsApplication;
import org.neo4j.gds.beta.filter.GraphStoreFilterService;
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.ProcedureTransactionService;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.TransactionContextService;
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserServices;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Here we keep everything related to constructing the {@link org.neo4j.gds.procedures.catalog.CatalogFacade}
 * from a {@link org.neo4j.kernel.api.procedure.Context}, at request time.
 * <p>
 * We can resolve things like user and database id here, construct termination flags, and such.
 */
public class CatalogFacadeFactory {
    // Global scoped/ global state things
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;

    // Request scoped things
    private final DatabaseIdService databaseIdService;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final KernelTransactionAccessor kernelTransactionAccessor;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TerminationFlagService terminationFlagService;
    private final UserLogServices userLogServices;
    private final UserServices userServices;

    // Business logic
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator;

    /**
     * We inject services here so that we may control and isolate access to dependencies.
     * Take {@link org.neo4j.gds.services.UserServices} for example.
     * Without it, I would have to stub out Neo4j's {@link org.neo4j.kernel.api.procedure.Context}, in a non-trivial,
     * ugly way. Now instead I can inject the user by stubbing out GDS' own little POJO service.
     */
    public CatalogFacadeFactory(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseIdService databaseIdService,
        ExporterBuildersProviderService exporterBuildersProviderService,
        KernelTransactionAccessor kernelTransactionAccessor,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserLogServices userLogServices,
        UserServices userServices,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;

        this.databaseIdService = databaseIdService;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.kernelTransactionAccessor = kernelTransactionAccessor;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.terminationFlagService = terminationFlagService;
        this.userLogServices = userLogServices;
        this.userServices = userServices;

        this.businessFacadeDecorator = businessFacadeDecorator;
    }

    /**
     * We construct the catalog facade at request time. At this point things like user and database id are set in stone.
     * And we can readily construct things like termination flags.
     */
    public CatalogFacade createCatalogFacade(Context context) {
        // Neo4j's services
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        // Derived data and services
        var databaseId = databaseIdService.getDatabaseId(graphDatabaseService);
        var procedureTransactionService = new ProcedureTransactionService(context);
        var procedureReturnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var streamCloser = new Consumer<AutoCloseable>() {
            @Override
            public void accept(AutoCloseable autoCloseable) {
                try (var statement = kernelTransaction.acquireStatement()) {
                    statement.registerCloseableResource(autoCloseable);
                }
            }
        };
        var terminationFlag = terminationFlagService.createTerminationFlag(kernelTransaction);
        var transactionContextService = new TransactionContextService();
        var user = userServices.getUser(context.securityContext());

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        // GDS services
        var configurationService = new ConfigurationService();
        var graphNameValidationService = new GraphNameValidationService();
        var graphProjectMemoryUsage = new GraphProjectMemoryUsageService(log, graphDatabaseService);
        var graphStoreFilterService = new GraphStoreFilterService();
        var graphStoreValidationService = new GraphStoreValidationService();

        // Exporter builders
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipPropertiesExporterBuilder = exportBuildersProvider.relationshipPropertiesExporterBuilder(
            exporterContext);
        var nodeLabelExporterBuilder = exportBuildersProvider.nodeLabelExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);

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
        var subGraphProjectApplication = new SubGraphProjectApplication(
            log,
            graphStoreFilterService,
            graphStoreCatalogService
        );
        var graphMemoryUsageApplication = new GraphMemoryUsageApplication(graphStoreCatalogService);
        var dropNodePropertiesApplication = new DropNodePropertiesApplication(log);
        var dropRelationshipsApplication = new DropRelationshipsApplication(log);
        var nodeLabelMutatorApplication = new NodeLabelMutatorApplication();
        var streamNodePropertiesApplication = new StreamNodePropertiesApplication(log);
        var streamRelationshipPropertiesApplication = new StreamRelationshipPropertiesApplication(log);
        var streamRelationshipsApplication = new StreamRelationshipsApplication();
        var writeNodePropertiesApplication = new WriteNodePropertiesApplication(log, nodePropertyExporterBuilder);
        var writeRelationshipPropertiesApplication = new WriteRelationshipPropertiesApplication(
            log,
            relationshipPropertiesExporterBuilder
        );
        var writeNodeLabelApplication = new WriteNodeLabelApplication(log, nodeLabelExporterBuilder);
        var writeRelationshipsApplication = new WriteRelationshipsApplication(log, relationshipExporterBuilder);
        var graphSamplingApplication = new GraphSamplingApplication(log, graphStoreCatalogService);
        var estimateCommonNeighbourAwareRandomWalkApplication = new EstimateCommonNeighbourAwareRandomWalkApplication();
        var generateGraphApplication = new GenerateGraphApplication(log, graphStoreCatalogService);

        // GDS business facade
        CatalogBusinessFacade businessFacade = new DefaultCatalogBusinessFacade(
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
            streamRelationshipsApplication,
            writeNodePropertiesApplication,
            writeRelationshipPropertiesApplication,
            writeNodeLabelApplication,
            writeRelationshipsApplication,
            graphSamplingApplication,
            estimateCommonNeighbourAwareRandomWalkApplication,
            generateGraphApplication
        );

        // wrap in decorator to inject conditional behaviour
        if (businessFacadeDecorator.isPresent()) {
            businessFacade = businessFacadeDecorator.get().apply(businessFacade);
        }

        return new CatalogFacade(
            streamCloser,
            databaseId,
            graphDatabaseService,
            procedureReturnColumns,
            procedureTransactionService,
            taskRegistryFactory,
            terminationFlag,
            transactionContextService,
            user,
            userLogRegistryFactory,
            userLogServices,
            businessFacade
        );
    }
}
