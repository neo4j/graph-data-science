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
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
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

    SimilarityProcedureProvider(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        boolean useMaxMemoryEstimation,
        AlgorithmMetaDataSetterService algorithmMetaDataSetterService,
        DatabaseIdAccessor databaseIdAccessor,
        KernelTransactionAccessor kernelTransactionAccessor,
        ExporterBuildersProviderService exporterBuildersProviderService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        AlgorithmMetricsService algorithmMetricsService,
        TerminationFlagService terminationFlagService,
        UserLogServices userLogServices,
        UserAccessor userAccessor
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.algorithmMetaDataSetterService = algorithmMetaDataSetterService;
        this.databaseIdAccessor = databaseIdAccessor;
        this.kernelTransactionAccessor = kernelTransactionAccessor;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.algorithmMetricsService = algorithmMetricsService;
        this.terminationFlagService = terminationFlagService;
        this.userLogServices = userLogServices;
        this.userAccessor = userAccessor;
    }

    SimilarityProcedureFacade createSimilarityProcedureFacade(Context context) throws ProcedureException {

        // Neo4j's services
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        // services derived from Neo4j's procedure context - request scoped dependencies
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var databaseId = databaseIdAccessor.getDatabaseId(context.graphDatabaseAPI());
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var user = userAccessor.getUser(context.securityContext());
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var terminationFlag = terminationFlagService.createTerminationFlag(kernelTransaction);
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(user)
            .with(terminationFlag)
            .build();

        // the business
        var algorithmRunner = new AlgorithmRunner(
            log,
            graphStoreCatalogService,
            algorithmMemoryValidationService,
            taskRegistryFactory,
            userLogRegistryFactory,
            algorithmMetricsService,
            requestScopedDependencies
        );

        // structural behaviour
        var similarityAlgorithmsFacade = new SimilarityAlgorithmsFacade(
            algorithmRunner
        );
        var streamBusinessFacade = new SimilarityAlgorithmsStreamBusinessFacade(similarityAlgorithmsFacade);

        // inject this one and have it shared, one day it won't have a JVM wide singleton backing it, fingers crossed
        var configurationParser = new ConfigurationParser(
            DefaultsConfiguration.Instance,
            LimitsConfiguration.Instance
        );

        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            log
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            user,
            graphLoaderContext
        );

        var estimateBusinessFacade = new SimilarityAlgorithmsEstimateBusinessFacade(
            new AlgorithmEstimator(
                graphStoreCatalogService,
                fictitiousGraphStoreEstimationService,
                databaseGraphStoreEstimationService,
                databaseId,
                user
            )
        );

        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);

        var mutateBusinessFacade = new SimilarityAlgorithmsMutateBusinessFacade(
            similarityAlgorithmsFacade,
            new MutateRelationshipService(log)
        );
        var statsBusinessFacade = new SimilarityAlgorithmsStatsBusinessFacade(similarityAlgorithmsFacade);
        var writeBusinessFacade = new SimilarityAlgorithmsWriteBusinessFacade(
            similarityAlgorithmsFacade,
            new WriteRelationshipService(log, relationshipExporterBuilder, taskRegistryFactory, terminationFlag)
        );
        return new SimilarityProcedureFacade(
            configurationParser,
            user,
            returnColumns,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade,
            estimateBusinessFacade,
            algorithmMetaDataSetter

        );
    }

}
