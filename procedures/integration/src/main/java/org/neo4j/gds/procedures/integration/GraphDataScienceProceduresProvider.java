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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.graphstorecatalog.ExportLocation;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.procedures.DatabaseIdAccessor;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.GraphCatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.LocalGraphDataScienceProcedures;
import org.neo4j.gds.procedures.ProcedureCallContextReturnColumns;
import org.neo4j.gds.procedures.ProcedureTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserAccessor;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.gds.procedures.pipelines.PipelineRepository;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Function;

/**
 * We use this at request time to construct the facade that the procedures call.
 */
public class GraphDataScienceProceduresProvider implements ThrowingFunction<Context, GraphDataScienceProcedures, ProcedureException> {
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final ProcedureTransactionAccessor procedureTransactionAccessor = new ProcedureTransactionAccessor();
    private final UserAccessor userAccessor = new UserAccessor();

    private final Log log;
    private final Configuration neo4jConfiguration;

    private final DefaultsConfiguration defaultsConfiguration;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final ExportLocation exportLocation;
    private final GraphCatalogProcedureFacadeFactory graphCatalogProcedureFacadeFactory;
    private final FeatureTogglesRepository featureTogglesRepository;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final LimitsConfiguration limitsConfiguration;
    private final MemoryGuard memoryGuard;
    private final Metrics metrics;
    private final ModelCatalog modelCatalog;
    private final ModelRepository modelRepository;
    private final PipelineRepository pipelineRepository;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TaskStoreService taskStoreService;
    private final UserLogServices userLogServices;

    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator;
    private final Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator;

    GraphDataScienceProceduresProvider(
        Log log,
        Configuration neo4jConfiguration,
        DefaultsConfiguration defaultsConfiguration,
        ExporterBuildersProviderService exporterBuildersProviderService,
        ExportLocation exportLocation,
        GraphCatalogProcedureFacadeFactory graphCatalogProcedureFacadeFactory,
        FeatureTogglesRepository featureTogglesRepository,
        GraphStoreCatalogService graphStoreCatalogService,
        LimitsConfiguration limitsConfiguration,
        MemoryGuard memoryGuard,
        Metrics metrics,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        PipelineRepository pipelineRepository,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TaskStoreService taskStoreService,
        UserLogServices userLogServices,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator
    ) {
        this.log = log;
        this.neo4jConfiguration = neo4jConfiguration;

        this.defaultsConfiguration = defaultsConfiguration;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.exportLocation = exportLocation;
        this.graphCatalogProcedureFacadeFactory = graphCatalogProcedureFacadeFactory;
        this.featureTogglesRepository = featureTogglesRepository;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.limitsConfiguration = limitsConfiguration;
        this.memoryGuard = memoryGuard;
        this.metrics = metrics;
        this.modelCatalog = modelCatalog;
        this.modelRepository = modelRepository;
        this.pipelineRepository = pipelineRepository;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.taskStoreService = taskStoreService;
        this.userLogServices = userLogServices;

        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.graphCatalogApplicationsDecorator = graphCatalogApplicationsDecorator;
        this.modelCatalogApplicationsDecorator = modelCatalogApplicationsDecorator;
    }

    @Override
    public GraphDataScienceProcedures apply(Context context) throws ProcedureException {
        var graphDatabaseService = context.graphDatabaseAPI();
        var dependencyResolver = graphDatabaseService.getDependencyResolver();
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);
        var procedureCallContext = context.procedureCallContext();
        var procedureTransaction = procedureTransactionAccessor.getProcedureTransaction(context);

        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);
        var procedureReturnColumns = new ProcedureCallContextReturnColumns(procedureCallContext);
        var terminationMonitor = new TransactionTerminationMonitor(kernelTransaction);
        var terminationFlag = TerminationFlag.wrap(terminationMonitor);
        var user = userAccessor.getUser(context.securityContext());
        var writeContext = createWriteContext(exporterContext, graphDatabaseService);

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var taskStore = taskStoreService.getTaskStore(databaseId);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);
        var userLogStore = userLogServices.getUserLogStore(databaseId);

        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            log
        );

        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(graphLoaderContext)
            .with(taskRegistryFactory)
            .with(taskStore)
            .with(terminationFlag)
            .with(user)
            .with(userLogRegistryFactory)
            .with(userLogStore)
            .build();

        return LocalGraphDataScienceProcedures.create(
            log,
            defaultsConfiguration,
            dependencyResolver,
            exportLocation,
            graphCatalogProcedureFacadeFactory,
            featureTogglesRepository,
            graphStoreCatalogService,
            limitsConfiguration,
            memoryGuard,
            metrics,
            modelCatalog,
            modelRepository,
            pipelineRepository,
            graphDatabaseService,
            kernelTransaction,
            procedureReturnColumns,
            requestScopedDependencies,
            terminationMonitor,
            procedureTransaction,
            writeContext,
            algorithmProcessingTemplateDecorator,
            graphCatalogApplicationsDecorator,
            modelCatalogApplicationsDecorator
        );
    }

    private WriteContext createWriteContext(
        ExporterContext exporterContext,
        GraphDatabaseService graphDatabaseService
    ) {
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(
            graphDatabaseService,
            neo4jConfiguration
        );

        return WriteContext.create(exportBuildersProvider, exporterContext);
    }
}
