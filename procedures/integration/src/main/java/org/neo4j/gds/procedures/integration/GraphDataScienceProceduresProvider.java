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
import org.neo4j.gds.LicenseDetails;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.compat.DatabaseIdSupplier;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.utils.logging.GdsLoggers;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.domain.services.GloballyScopedDependencies;
import org.neo4j.gds.executor.MemoryEstimationContext;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;
import org.neo4j.gds.metrics.telemetry.TelemetryLoggerImpl;
import org.neo4j.gds.procedures.GraphCatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.LocalGraphDataScienceProcedures;
import org.neo4j.gds.procedures.ProcedureCallContextReturnColumns;
import org.neo4j.gds.procedures.ProcedureTransactionAccessor;
import org.neo4j.gds.procedures.RequestCorrelationIdAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserAccessor;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.gds.procedures.pipelines.PipelineRepository;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;
import org.neo4j.gds.settings.GdsSettings;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * We use this at request time to construct the facade that the procedures call.
 */
public class GraphDataScienceProceduresProvider implements ThrowingFunction<Context, GraphDataScienceProcedures, ProcedureException> {
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final ProcedureTransactionAccessor procedureTransactionAccessor = new ProcedureTransactionAccessor();
    private final RequestCorrelationIdAccessor requestCorrelationIdAccessor = new RequestCorrelationIdAccessor();
    private final UserAccessor userAccessor;

    private final GdsLoggers loggers;
    private final Configuration neo4jConfiguration;

    private final OpenGraphDataScienceSpecifics openGraphDataScienceSpecifics;
    private final GloballyScopedDependencies globallyScopedDependencies;
    private final DefaultsConfiguration defaultsConfiguration;
    private final GraphCatalogProcedureFacadeFactory graphCatalogProcedureFacadeFactory;
    private final GraphStoreFactorySuppliers graphStoreFactorySuppliers;
    private final FeatureTogglesRepository featureTogglesRepository;
    private final LicenseDetails licenseDetails;
    private final LimitsConfiguration limitsConfiguration;
    private final MemoryGuard memoryGuard;
    private final MemoryEstimationContext memoryEstimationContext;
    private final PipelineRepository pipelineRepository;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TaskStoreService taskStoreService;
    private final UserLogServices userLogServices;

    private final MemoryTracker memoryTracker;

    GraphDataScienceProceduresProvider(
        UserAccessor userAccessor,
        GdsLoggers loggers,
        Configuration neo4jConfiguration,
        OpenGraphDataScienceSpecifics openGraphDataScienceSpecifics,
        GloballyScopedDependencies globallyScopedDependencies,
        DefaultsConfiguration defaultsConfiguration,
        GraphCatalogProcedureFacadeFactory graphCatalogProcedureFacadeFactory,
        FeatureTogglesRepository featureTogglesRepository,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        LicenseDetails licenseDetails,
        LimitsConfiguration limitsConfiguration,
        MemoryGuard memoryGuard,
        MemoryEstimationContext memoryEstimationContext,
        PipelineRepository pipelineRepository,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TaskStoreService taskStoreService,
        UserLogServices userLogServices,
        MemoryTracker memoryTracker
    ) {
        this.userAccessor = userAccessor;

        this.loggers = loggers;
        this.neo4jConfiguration = neo4jConfiguration;

        this.openGraphDataScienceSpecifics = openGraphDataScienceSpecifics;
        this.globallyScopedDependencies = globallyScopedDependencies;
        this.defaultsConfiguration = defaultsConfiguration;
        this.graphCatalogProcedureFacadeFactory = graphCatalogProcedureFacadeFactory;
        this.graphStoreFactorySuppliers = graphStoreFactorySuppliers;
        this.featureTogglesRepository = featureTogglesRepository;
        this.licenseDetails = licenseDetails;
        this.limitsConfiguration = limitsConfiguration;
        this.memoryGuard = memoryGuard;
        this.memoryEstimationContext = memoryEstimationContext;
        this.pipelineRepository = pipelineRepository;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.taskStoreService = taskStoreService;
        this.userLogServices = userLogServices;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public GraphDataScienceProcedures apply(Context context) throws ProcedureException {
        var dependencyResolver = context.dependencyResolver();
        var graphDatabaseService = context.graphDatabaseAPI();
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);
        var procedureCallContext = context.procedureCallContext();
        var procedureTransaction = procedureTransactionAccessor.getProcedureTransaction(context);

        var databaseId = new DatabaseIdSupplier().databaseId(context);
        var procedureReturnColumns = new ProcedureCallContextReturnColumns(procedureCallContext);
        var requestCorrelationId = requestCorrelationIdAccessor.getRequestCorrelationId(kernelTransaction);
        var terminationMonitor = new TransactionTerminationMonitor(kernelTransaction);
        var terminationFlag = TerminationFlag.wrap(terminationMonitor);
        var user = userAccessor.getUser(context.securityContext());
        var writeContext = createWriteContext(exporterContext, graphDatabaseService);

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var taskStore = taskStoreService.getOrCreateTaskStore(databaseId);
        taskStore.addListener(memoryTracker);

        var userLogRegistry = userLogServices.getUserLogRegistry(databaseId, user);
        var userLogStore = userLogServices.getUserLogStore(databaseId);

        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistry,
            loggers.log()
        );

        var requestScopedDependencies = RequestScopedDependencies.builder()
            .correlationId(requestCorrelationId)
            .databaseId(databaseId)
            .graphLoaderContext(graphLoaderContext)
            .taskRegistryFactory(taskRegistryFactory)
            .taskStore(taskStore)
            .terminationFlag(terminationFlag)
            .user(user)
            .userLogRegistry(userLogRegistry)
            .userLogStore(userLogStore)
            .build();

        var telemetryLogger = neo4jConfiguration.get(GdsSettings.telemetryLoggingEnabled()) ?
            new TelemetryLoggerImpl(loggers.log()) :
            TelemetryLogger.DISABLED;

        return LocalGraphDataScienceProcedures.create(
            loggers,
            globallyScopedDependencies,
            telemetryLogger,
            defaultsConfiguration,
            openGraphDataScienceSpecifics.exportLocation(),
            graphCatalogProcedureFacadeFactory,
            featureTogglesRepository,
            graphStoreFactorySuppliers,
            licenseDetails,
            limitsConfiguration,
            memoryGuard,
            memoryEstimationContext,
            openGraphDataScienceSpecifics.metrics(),
            openGraphDataScienceSpecifics.modelRepository(),
            pipelineRepository,
            graphDatabaseService,
            kernelTransaction,
            procedureReturnColumns,
            requestScopedDependencies,
            dependencyResolver,
            terminationMonitor,
            procedureTransaction,
            writeContext,
            openGraphDataScienceSpecifics.algorithmProcessingTemplateDecorator(),
            openGraphDataScienceSpecifics.graphCatalogApplicationsDecorator(),
            openGraphDataScienceSpecifics.modelCatalogApplicationsDecorator(),
            memoryTracker
        );
    }

    private WriteContext createWriteContext(
        ExporterContext exporterContext,
        GraphDatabaseService graphDatabaseService
    ) {
        var exportBuildersProvider = openGraphDataScienceSpecifics.exporterBuildersProviderService()
            .identifyExportBuildersProvider(
                graphDatabaseService,
                neo4jConfiguration
            );

        return WriteContext.create(exportBuildersProvider, exporterContext);
    }
}
