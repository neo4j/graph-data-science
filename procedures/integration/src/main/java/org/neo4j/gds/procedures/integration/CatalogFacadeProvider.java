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
import org.neo4j.gds.applications.graphstorecatalog.CatalogConfigurationService;
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
import org.neo4j.gds.core.loading.GraphProjectCypherResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.ProcedureTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.TransactionContextAccessor;
import org.neo4j.gds.procedures.catalog.CatalogFacade;
import org.neo4j.gds.projection.GraphProjectNativeResult;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Here we keep everything related to constructing the {@link org.neo4j.gds.procedures.catalog.CatalogFacade}
 * from a {@link org.neo4j.kernel.api.procedure.Context}, at request time.
 * <p>
 * We can resolve things like user and database id here, construct termination flags, and such.
 * <p>
 * We call it a provider because it is used as a sub-provider to the {@link org.neo4j.gds.procedures.GraphDataScience} provider.
 */
public class CatalogFacadeProvider {
    // Global scoped/ global state/ stateless things
    private final CatalogConfigurationService catalogConfigurationService;
    private final Log log;
    private final GraphNameValidationService graphNameValidationService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final GraphStoreValidationService graphStoreValidationService;
    private final ProcedureTransactionAccessor procedureTransactionAccessor;

    // Request scoped things
    private final DatabaseIdAccessor databaseIdAccessor;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final KernelTransactionAccessor kernelTransactionAccessor;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TerminationFlagService terminationFlagService;
    private final TransactionContextAccessor transactionContextAccessor;
    private final UserLogServices userLogServices;
    private final UserAccessor userAccessor;

    // applications
    private final DropGraphApplication dropGraphApplication;
    private final ListGraphApplication listGraphApplication;
    private final SubGraphProjectApplication subGraphProjectApplication;
    private final GraphMemoryUsageApplication graphMemoryUsageApplication;
    private final DropNodePropertiesApplication dropNodePropertiesApplication;
    private final DropRelationshipsApplication dropRelationshipsApplication;
    private final NodeLabelMutatorApplication nodeLabelMutatorApplication;
    private final StreamNodePropertiesApplication streamNodePropertiesApplication;
    private final StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication;
    private final StreamRelationshipsApplication streamRelationshipsApplication;
    private final GraphSamplingApplication graphSamplingApplication;
    private final EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication;
    private final GenerateGraphApplication generateGraphApplication;

    // Business logic
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator;

    /**
     * We inject services here so that we may control and isolate access to dependencies.
     * Take {@link org.neo4j.gds.services.UserAccessor} for example.
     * Without it, I would have to stub out Neo4j's {@link org.neo4j.kernel.api.procedure.Context}, in a non-trivial,
     * ugly way. Now instead I can inject the user by stubbing out GDS' own little POJO service.
     */
    CatalogFacadeProvider(
        CatalogConfigurationService catalogConfigurationService,
        Log log,
        GraphNameValidationService graphNameValidationService,
        GraphStoreCatalogService graphStoreCatalogService,
        GraphStoreValidationService graphStoreValidationService,
        ProcedureTransactionAccessor procedureTransactionAccessor,
        DatabaseIdAccessor databaseIdAccessor,
        ExporterBuildersProviderService exporterBuildersProviderService,
        KernelTransactionAccessor kernelTransactionAccessor,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        TransactionContextAccessor transactionContextAccessor,
        UserLogServices userLogServices,
        UserAccessor userAccessor,
        DropGraphApplication dropGraphApplication,
        DropNodePropertiesApplication dropNodePropertiesApplication,
        DropRelationshipsApplication dropRelationshipsApplication,
        EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication,
        GenerateGraphApplication generateGraphApplication,
        GraphMemoryUsageApplication graphMemoryUsageApplication,
        GraphSamplingApplication graphSamplingApplication,
        ListGraphApplication listGraphApplication,
        NodeLabelMutatorApplication nodeLabelMutatorApplication,
        StreamNodePropertiesApplication streamNodePropertiesApplication,
        StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication,
        StreamRelationshipsApplication streamRelationshipsApplication,
        SubGraphProjectApplication subGraphProjectApplication,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator
    ) {
        this.catalogConfigurationService = catalogConfigurationService;
        this.graphNameValidationService = graphNameValidationService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.graphStoreValidationService = graphStoreValidationService;
        this.log = log;
        this.procedureTransactionAccessor = procedureTransactionAccessor;

        this.databaseIdAccessor = databaseIdAccessor;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.kernelTransactionAccessor = kernelTransactionAccessor;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.terminationFlagService = terminationFlagService;
        this.transactionContextAccessor = transactionContextAccessor;
        this.userLogServices = userLogServices;
        this.userAccessor = userAccessor;

        this.dropGraphApplication = dropGraphApplication;
        this.listGraphApplication = listGraphApplication;
        this.subGraphProjectApplication = subGraphProjectApplication;
        this.graphMemoryUsageApplication = graphMemoryUsageApplication;
        this.dropNodePropertiesApplication = dropNodePropertiesApplication;
        this.dropRelationshipsApplication = dropRelationshipsApplication;
        this.nodeLabelMutatorApplication = nodeLabelMutatorApplication;
        this.streamNodePropertiesApplication = streamNodePropertiesApplication;
        this.streamRelationshipPropertiesApplication = streamRelationshipPropertiesApplication;
        this.streamRelationshipsApplication = streamRelationshipsApplication;
        this.graphSamplingApplication = graphSamplingApplication;
        this.estimateCommonNeighbourAwareRandomWalkApplication = estimateCommonNeighbourAwareRandomWalkApplication;
        this.generateGraphApplication = generateGraphApplication;

        this.businessFacadeDecorator = businessFacadeDecorator;
    }

    /**
     * We construct the catalog facade at request time. At this point things like user and database id are set in stone.
     * And we can readily construct things like termination flags.
     */
    CatalogFacade createCatalogFacade(Context context) {
        // Neo4j's basic request scoped services
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);
        var procedureTransaction = procedureTransactionAccessor.getProcedureTransaction(context);

        // Derived data and services
        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);
        var graphProjectMemoryUsage = new GraphProjectMemoryUsageService(log, graphDatabaseService);
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
        var transactionContext = transactionContextAccessor.transactionContext(
            graphDatabaseService,
            procedureTransaction
        );
        var user = userAccessor.getUser(context.securityContext());
        var userLogStore = userLogServices.getUserLogStore(databaseId);

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        // Exporter builders
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipPropertiesExporterBuilder = exportBuildersProvider.relationshipPropertiesExporterBuilder(
            exporterContext);
        var nodeLabelExporterBuilder = exportBuildersProvider.nodeLabelExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);

        // GDS applications
        var nativeProjectApplication = new NativeProjectApplication(
            new GenericProjectApplication<>(
                log,
                graphStoreCatalogService,
                graphProjectMemoryUsage, // request scope so need to change
                GraphProjectNativeResult.Builder::new
            ), graphProjectMemoryUsage
        );
        var cypherProjectApplication = new CypherProjectApplication(
            new GenericProjectApplication<>(
                log,
                graphStoreCatalogService,
                graphProjectMemoryUsage, // request scope so need to change
                GraphProjectCypherResult.Builder::new
            ), graphProjectMemoryUsage
        );
        // request scope so need to change
        var writeNodePropertiesApplication = new WriteNodePropertiesApplication(log, nodePropertyExporterBuilder);
        var writeRelationshipPropertiesApplication = new WriteRelationshipPropertiesApplication(
            log,
            relationshipPropertiesExporterBuilder
        );
        var writeNodeLabelApplication = new WriteNodeLabelApplication(log, nodeLabelExporterBuilder);
        var writeRelationshipsApplication = new WriteRelationshipsApplication(log, relationshipExporterBuilder);

        // GDS business facade
        CatalogBusinessFacade businessFacade = new DefaultCatalogBusinessFacade(
            log,
            catalogConfigurationService,
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
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            user,
            userLogRegistryFactory,
            userLogStore,
            businessFacade
        );
    }
}
