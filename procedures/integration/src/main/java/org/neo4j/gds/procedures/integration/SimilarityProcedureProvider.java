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
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.algorithms.metrics.AlgorithmMetricsService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.similarity.MutateRelationshipService;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.configparser.ConfigurationParser;
import org.neo4j.gds.procedures.similarity.SimilarityProcedureFacade;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * We call it a provider because it is used as a sub-provider to the {@link org.neo4j.gds.procedures.GraphDataScience} provider.
 */
public class SimilarityProcedureProvider {
    /**
     * Simple, stateless services
     */
    private final FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();

    // Global state and services
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final boolean useMaxMemoryEstimation;

    // Request scoped state and services
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService;
    private final DatabaseIdAccessor databaseIdAccessor;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final KernelTransactionAccessor kernelTransactionAccessor;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final TerminationFlagService terminationFlagService;
    private final UserLogServices userLogServices;
    private final UserAccessor userAccessor;
    private final ConfigurationParser configurationParser;

    SimilarityProcedureProvider(
        Log log,
        ConfigurationParser configurationParser,
        GraphStoreCatalogService graphStoreCatalogService,
        boolean useMaxMemoryEstimation,
        AlgorithmMetaDataSetterService algorithmMetaDataSetterService,
        AlgorithmMetricsService algorithmMetricsService,
        DatabaseIdAccessor databaseIdAccessor,
        ExporterBuildersProviderService exporterBuildersProviderService,
        KernelTransactionAccessor kernelTransactionAccessor,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserAccessor userAccessor, UserLogServices userLogServices
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.algorithmMetaDataSetterService = algorithmMetaDataSetterService;
        this.algorithmMetricsService = algorithmMetricsService;
        this.configurationParser = configurationParser;
        this.databaseIdAccessor = databaseIdAccessor;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.kernelTransactionAccessor = kernelTransactionAccessor;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.terminationFlagService = terminationFlagService;
        this.userAccessor = userAccessor;
        this.userLogServices = userLogServices;
    }

    SimilarityProcedureFacade createSimilarityProcedureFacade(Context context) throws ProcedureException {
        // Neo4j's services
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        // services derived from Neo4j's procedure context - request scoped dependencies
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var databaseId = databaseIdAccessor.getDatabaseId(context.graphDatabaseAPI());
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var user = userAccessor.getUser(context.securityContext());
        var configurationCreator = new ConfigurationCreator(configurationParser, algorithmMetaDataSetter, user);
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var terminationFlag = terminationFlagService.createTerminationFlag(kernelTransaction);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);
        var writeRelationshipService = new WriteRelationshipService(
            log,
            relationshipExporterBuilder,
            taskRegistryFactory,
            terminationFlag
        );
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
            .with(user)
            .with(terminationFlag)
            .build();
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(graphLoaderContext, user);
        var algorithmEstimator = new AlgorithmEstimator(
            graphStoreCatalogService,
            fictitiousGraphStoreEstimationService,
            databaseGraphStoreEstimationService,
            databaseId,
            user
        );

        // the business
        var algorithmRunner = new AlgorithmRunner(
            log,
            graphStoreCatalogService,
            algorithmMetricsService,
            algorithmMemoryValidationService,
            requestScopedDependencies,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        // algorithms facade
        var similarityAlgorithmsFacade = new SimilarityAlgorithmsFacade(algorithmRunner);

        // mode-specific facades
        var estimateBusinessFacade = new SimilarityAlgorithmsEstimateBusinessFacade(algorithmEstimator);
        var mutateBusinessFacade = new SimilarityAlgorithmsMutateBusinessFacade(
            similarityAlgorithmsFacade,
            new MutateRelationshipService(log)
        );
        var statsBusinessFacade = new SimilarityAlgorithmsStatsBusinessFacade(similarityAlgorithmsFacade);
        var streamBusinessFacade = new SimilarityAlgorithmsStreamBusinessFacade(similarityAlgorithmsFacade);
        var writeBusinessFacade = new SimilarityAlgorithmsWriteBusinessFacade(
            similarityAlgorithmsFacade,
            writeRelationshipService
        );

        // procedure facade
        return new SimilarityProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }
}
