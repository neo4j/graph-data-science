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
import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.DefaultAlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.DefaultMemoryGuard;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.GraphDataScienceProceduresBuilder;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagAccessor;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Function;

/**
 * We use this at request time to construct the facade that the procedures call.
 */
public class GraphDataScienceProvider implements ThrowingFunction<Context, GraphDataScienceProcedures, ProcedureException> {
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final PipelinesProcedureFacadeProvider pipelinesProcedureFacadeProvider = new PipelinesProcedureFacadeProvider();
    private final TerminationFlagAccessor terminationFlagAccessor = new TerminationFlagAccessor();
    private final UserAccessor userAccessor = new UserAccessor();

    private final Log log;
    private final AlgorithmFacadeFactoryProvider algorithmFacadeFactoryProvider;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator;
    private final CatalogFacadeProvider catalogFacadeProvider;
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
        AlgorithmFacadeFactoryProvider algorithmFacadeFactoryProvider,
        AlgorithmMetricsService algorithmMetricsService,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        CatalogFacadeProvider catalogFacadeProvider,
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
        this.algorithmFacadeFactoryProvider = algorithmFacadeFactoryProvider;
        this.algorithmMetricsService = algorithmMetricsService;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.catalogBusinessFacadeDecorator = catalogBusinessFacadeDecorator;
        this.catalogFacadeProvider = catalogFacadeProvider;
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
        // with the context we can build the application layer before any of the procedure stuff
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);
        var terminationFlag = terminationFlagAccessor.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(taskRegistryFactory)
            .with(terminationFlag)
            .with(userLogRegistryFactory)
            .with(user)
            .build();

        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipStreamExporterBuilder = exportBuildersProvider.relationshipStreamExporterBuilder(exporterContext);

        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            log
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            graphLoaderContext,
            user
        );

        // this eventually goes below the fold, inside the application layer
        var algorithmEstimationTemplate = new AlgorithmEstimationTemplate(
            graphStoreCatalogService,
            databaseGraphStoreEstimationService,
            requestScopedDependencies
        );

        var algorithmProcessingTemplate = buildAlgorithmProcessingTemplate(requestScopedDependencies);

        var applicationsFacade = ApplicationsFacade.create(
            log,
            catalogBusinessFacadeDecorator,
            graphStoreCatalogService,
            projectionMetricsService,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            requestScopedDependencies
        );

        var catalogProcedureFacade = catalogFacadeProvider.createCatalogProcedureFacade(applicationsFacade, context);

        var algorithmFacadeFactory = algorithmFacadeFactoryProvider.createAlgorithmFacadeFactory(
            context,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            requestScopedDependencies,
            kernelTransaction,
            graphDatabaseService,
            databaseGraphStoreEstimationService
        );
        var centralityProcedureFacade = algorithmFacadeFactory.createCentralityProcedureFacade();
        var communityProcedureFacade = algorithmFacadeFactory.createCommunityProcedureFacade();
        var miscAlgorithmsProcedureFacade = algorithmFacadeFactory.createMiscellaneousProcedureFacade();
        var nodeEmbeddingsProcedureFacade = algorithmFacadeFactory.createNodeEmbeddingsProcedureFacade();
        var pathFindingProcedureFacade = algorithmFacadeFactory.createPathFindingProcedureFacade(applicationsFacade);
        var similarityProcedureFacade = algorithmFacadeFactory.createSimilarityProcedureFacade();

        var pipelinesProcedureFacade = pipelinesProcedureFacadeProvider.createPipelinesProcedureFacade(context);

        return new GraphDataScienceProceduresBuilder(log)
            .with(catalogProcedureFacade)
            .with(centralityProcedureFacade)
            .with(communityProcedureFacade)
            .with(miscAlgorithmsProcedureFacade)
            .with(nodeEmbeddingsProcedureFacade)
            .with(pathFindingProcedureFacade)
            .with(pipelinesProcedureFacade)
            .with(similarityProcedureFacade)
            .with(deprecatedProceduresMetricService)
            .build();
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
