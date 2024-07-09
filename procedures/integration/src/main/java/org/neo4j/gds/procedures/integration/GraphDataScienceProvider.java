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

import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.applications.algorithms.embeddings.GraphSageModelRepository;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.procedures.AlgorithmProcedureFacadeBuilderFactory;
import org.neo4j.gds.procedures.CatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.DatabaseIdAccessor;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.ProcedureCallContextReturnColumns;
import org.neo4j.gds.procedures.ProcedureTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagAccessor;
import org.neo4j.gds.procedures.UserAccessor;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Function;

/**
 * We use this at request time to construct the facade that the procedures call.
 */
public class GraphDataScienceProvider implements ThrowingFunction<Context, GraphDataScienceProcedures, ProcedureException> {
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService = new AlgorithmMetaDataSetterService();
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final ProcedureTransactionAccessor procedureTransactionAccessor = new ProcedureTransactionAccessor();
    private final TerminationFlagAccessor terminationFlagAccessor = new TerminationFlagAccessor();
    private final UserAccessor userAccessor = new UserAccessor();

    private final Log log;
    private final DefaultsConfiguration defaultsConfiguration;
    private final LimitsConfiguration limitsConfiguration;
    private final AlgorithmProcedureFacadeBuilderFactory algorithmProcedureFacadeBuilderFactory;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator;
    private final CatalogProcedureFacadeFactory catalogProcedureFacadeFactory;
    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final MemoryGuard memoryGuard;
    private final ProjectionMetricsService projectionMetricsService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final Config config;
    private final ModelCatalog modelCatalog;
    private final GraphSageModelRepository graphSageModelRepository;

    GraphDataScienceProvider(
        Log log,
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        AlgorithmProcedureFacadeBuilderFactory algorithmProcedureFacadeBuilderFactory,
        AlgorithmMetricsService algorithmMetricsService,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        CatalogProcedureFacadeFactory catalogProcedureFacadeFactory,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService,
        ExporterBuildersProviderService exporterBuildersProviderService,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        ProjectionMetricsService projectionMetricsService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        Config config,
        ModelCatalog modelCatalog,
        GraphSageModelRepository graphSageModelRepository
    ) {
        this.log = log;
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;
        this.algorithmProcedureFacadeBuilderFactory = algorithmProcedureFacadeBuilderFactory;
        this.algorithmMetricsService = algorithmMetricsService;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.catalogBusinessFacadeDecorator = catalogBusinessFacadeDecorator;
        this.catalogProcedureFacadeFactory = catalogProcedureFacadeFactory;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.memoryGuard = memoryGuard;
        this.projectionMetricsService = projectionMetricsService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.config = config;
        this.modelCatalog = modelCatalog;
        this.graphSageModelRepository = graphSageModelRepository;
    }

    @Override
    public GraphDataScienceProcedures apply(Context context) throws ProcedureException {
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);
        var procedureCallContext = context.procedureCallContext();

        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var graphDatabaseService = context.graphDatabaseAPI();
        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);

        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(
            graphDatabaseService,
            config
        );
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodeLabelExporterBuilder = exportBuildersProvider.nodeLabelExporterBuilder(exporterContext);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipPropertiesExporterBuilder = exportBuildersProvider.relationshipPropertiesExporterBuilder(
            exporterContext);
        var relationshipStreamExporterBuilder = exportBuildersProvider.relationshipStreamExporterBuilder(exporterContext);

        var procedureReturnColumns = new ProcedureCallContextReturnColumns(procedureCallContext);
        var terminationFlag = terminationFlagAccessor.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
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

        var procedureContext = WriteContext.builder()
            .with(nodeLabelExporterBuilder)
            .with(nodePropertyExporterBuilder)
            .with(relationshipExporterBuilder)
            .with(relationshipPropertiesExporterBuilder)
            .with(relationshipStreamExporterBuilder)
            .build();

        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(graphLoaderContext)
            .with(taskRegistryFactory)
            .with(terminationFlag)
            .with(user)
            .with(userLogRegistryFactory)
            .with(userLogStore)
            .build();

        return GraphDataScienceProcedures.create(
            log,
            defaultsConfiguration,
            limitsConfiguration,
            algorithmProcessingTemplateDecorator,
            catalogBusinessFacadeDecorator,
            graphStoreCatalogService,
            memoryGuard,
            algorithmMetricsService,
            projectionMetricsService,
            algorithmMetaDataSetter,
            kernelTransaction,
            requestScopedDependencies,
            procedureContext,
            procedureReturnColumns,
            catalogProcedureFacadeFactory,
            graphDatabaseService,
            procedureTransactionAccessor.getProcedureTransaction(context),
            algorithmProcedureFacadeBuilderFactory,
            deprecatedProceduresMetricService,
            modelCatalog,
            graphSageModelRepository
        );
    }
}
