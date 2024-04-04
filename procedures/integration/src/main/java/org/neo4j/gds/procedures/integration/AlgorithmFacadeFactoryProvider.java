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
import org.neo4j.gds.TransactionCloseableResourceRegistry;
import org.neo4j.gds.TransactionNodeLookup;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.similarity.MutateRelationshipService;
import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.DefaultAlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.DefaultMemoryGuard;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.modelcatalogservices.ModelCatalogServiceProvider;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagAccessor;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Function;

class AlgorithmFacadeFactoryProvider {
    // dull utilities
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService = new AlgorithmMetaDataSetterService();
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final TerminationFlagAccessor terminationFlagAccessor = new TerminationFlagAccessor();
    private final UserAccessor userAccessor = new UserAccessor();

    // Global state and services
    private final Log log;
    private final ConfigurationParser configurationParser;
    private final DefaultsConfiguration defaultsConfiguration;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final LimitsConfiguration limitsConfiguration;
    private final MemoryGauge memoryGauge;
    private final boolean useMaxMemoryEstimation;

    // Request scoped state and services
    private final AlgorithmMetricsService algorithmMetricsService;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final ModelCatalogServiceProvider modelCatalogServiceProvider;
    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    //algorithm facade parameters

    AlgorithmFacadeFactoryProvider(
        Log log,
        ConfigurationParser configurationParser,
        DefaultsConfiguration defaultsConfiguration,
        GraphStoreCatalogService graphStoreCatalogService,
        LimitsConfiguration limitsConfiguration,
        MemoryGauge memoryGauge,
        boolean useMaxMemoryEstimation,
        AlgorithmMetricsService algorithmMetricsService,
        ExporterBuildersProviderService exporterBuildersProviderService,
        ModelCatalogServiceProvider modelCatalogServiceProvider,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator
    ) {
        this.log = log;
        this.configurationParser = configurationParser;
        this.defaultsConfiguration = defaultsConfiguration;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.limitsConfiguration = limitsConfiguration;
        this.memoryGauge = memoryGauge;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;

        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.algorithmMetricsService = algorithmMetricsService;
        this.userLogServices = userLogServices;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.modelCatalogServiceProvider = modelCatalogServiceProvider;
    }


    AlgorithmFacadeFactory createAlgorithmFacadeFactory(Context context) throws ProcedureException {
        // Neo4j's services
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        /*
         * GDS services derived from Procedure Context.
         * These come in layers, we can create some services readily,
         * but others need some of our own products and come later.
         * I have tried to mark those layers in comments below.
         */
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);
        var databaseId = databaseIdAccessor.getDatabaseId(context.graphDatabaseAPI());
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var memoryGuard = new DefaultMemoryGuard(log, useMaxMemoryEstimation, memoryGauge);
        var mutateNodePropertyService = new MutateNodePropertyService(log);
        var mutateRelationshipService = new MutateRelationshipService(log);
        var nodeLookup = new TransactionNodeLookup(kernelTransaction);
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var terminationFlag = terminationFlagAccessor.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        // Second layer
        var configurationCreator = new ConfigurationCreator(configurationParser, algorithmMetaDataSetter, user);
        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            log
        );
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipStreamExporterBuilder = exportBuildersProvider.relationshipStreamExporterBuilder(exporterContext);
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(user)
            .with(terminationFlag)
            .build();

        // Third layer
        var writeNodePropertyService = new WriteNodePropertyService(
            log,
            nodePropertyExporterBuilder,
            taskRegistryFactory,
            terminationFlag
        );
        var writeRelationshipService = new WriteRelationshipService(
            log,
            relationshipExporterBuilder,
            taskRegistryFactory,
            terminationFlag
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(graphLoaderContext, user);

        // Fourth layer
        var algorithmEstimator = new AlgorithmEstimator(
            graphStoreCatalogService,
            fictitiousGraphStoreEstimationService,
            databaseGraphStoreEstimationService,
            databaseId,
            user
        );
        var algorithmRunner = new AlgorithmRunner(
            log,
            graphStoreCatalogService,
            algorithmMetricsService,
            algorithmMemoryValidationService,
            requestScopedDependencies,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        // business facades
        AlgorithmProcessingTemplate algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            log,
            algorithmMetricsService,
            graphStoreCatalogService,
            memoryGuard,
            databaseId,
            user
        );

        if (this.algorithmProcessingTemplateDecorator.isPresent())
            algorithmProcessingTemplate = this.algorithmProcessingTemplateDecorator.get().apply(
                algorithmProcessingTemplate);

        var algorithmEstimationTemplate = new AlgorithmEstimationTemplate(
            graphStoreCatalogService,
            databaseId,
            databaseGraphStoreEstimationService,
            user
        );

        // procedure facade
        return new AlgorithmFacadeFactory(
            log,
            defaultsConfiguration,
            limitsConfiguration,
            closeableResourceRegistry,
            configurationCreator,
            configurationParser,
            nodeLookup,
            returnColumns,
            mutateNodePropertyService,
            writeNodePropertyService,
            mutateRelationshipService,
            writeRelationshipService,
            algorithmRunner,
            algorithmEstimator,
            algorithmProcessingTemplate,
            algorithmEstimationTemplate,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            user,
            userLogRegistryFactory,
            modelCatalogServiceProvider.createService(graphDatabaseService, log)
        );
    }
}
