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
import org.neo4j.gds.applications.algorithms.machinery.DefaultAlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.DefaultMemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.procedures.AlgorithmFacadeBuilderFactory;
import org.neo4j.gds.procedures.CatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.DatabaseIdAccessor;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
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
    private final AlgorithmFacadeBuilderFactory algorithmFacadeBuilderFactory;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator;
    private final CatalogProcedureFacadeFactory catalogProcedureFacadeFactory;
    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final MemoryGauge memoryGauge;
    private final ProjectionMetricsService projectionMetricsService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final boolean useMaxMemoryEstimation;
    private final UserLogServices userLogServices;

    GraphDataScienceProvider(
        Log log,
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        AlgorithmFacadeBuilderFactory algorithmFacadeBuilderFactory,
        AlgorithmMetricsService algorithmMetricsService,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        CatalogProcedureFacadeFactory catalogProcedureFacadeFactory,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService,
        ExporterBuildersProviderService exporterBuildersProviderService,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGauge memoryGauge,
        ProjectionMetricsService projectionMetricsService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        boolean useMaxMemoryEstimation,
        UserLogServices userLogServices
    ) {
        this.log = log;
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;
        this.algorithmFacadeBuilderFactory = algorithmFacadeBuilderFactory;
        this.algorithmMetricsService = algorithmMetricsService;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.catalogBusinessFacadeDecorator = catalogBusinessFacadeDecorator;
        this.catalogProcedureFacadeFactory = catalogProcedureFacadeFactory;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.memoryGauge = memoryGauge;
        this.projectionMetricsService = projectionMetricsService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.userLogServices = userLogServices;
    }

    @Override
    public GraphDataScienceProcedures apply(Context context) throws ProcedureException {
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var graphDatabaseService = context.graphDatabaseAPI();
        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);

        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipStreamExporterBuilder = exportBuildersProvider.relationshipStreamExporterBuilder(exporterContext);
        var terminationFlag = terminationFlagAccessor.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(nodePropertyExporterBuilder)
            .with(relationshipExporterBuilder)
            .with(relationshipStreamExporterBuilder)
            .with(taskRegistryFactory)
            .with(terminationFlag)
            .with(userLogRegistryFactory)
            .with(user)
            .build();

        var algorithmProcessingTemplate = buildAlgorithmProcessingTemplate(requestScopedDependencies);
        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            log
        );

        return GraphDataScienceProcedures.create(
            log,
            defaultsConfiguration,
            limitsConfiguration,
            catalogBusinessFacadeDecorator,
            graphStoreCatalogService,
            projectionMetricsService,
            algorithmMetaDataSetter,
            algorithmProcessingTemplate,
            kernelTransaction,
            graphLoaderContext,
            context.procedureCallContext(),
            requestScopedDependencies,
            catalogProcedureFacadeFactory,
            context.securityContext(),
            exporterContext,
            graphDatabaseService,
            procedureTransactionAccessor.getProcedureTransaction(context),
            algorithmFacadeBuilderFactory,
            deprecatedProceduresMetricService
        );
    }

    private AlgorithmProcessingTemplate buildAlgorithmProcessingTemplate(RequestScopedDependencies requestScopedDependencies) {
        var memoryGuard = new DefaultMemoryGuard(log, useMaxMemoryEstimation, memoryGauge);

        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            log,
            algorithmMetricsService,
            graphStoreCatalogService,
            memoryGuard,
            requestScopedDependencies
        );

        if (algorithmProcessingTemplateDecorator.isEmpty()) return algorithmProcessingTemplate;

        return algorithmProcessingTemplateDecorator.get().apply(algorithmProcessingTemplate);
    }
}
