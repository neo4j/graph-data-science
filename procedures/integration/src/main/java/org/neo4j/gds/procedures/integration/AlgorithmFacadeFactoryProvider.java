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
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.modelcatalogservices.ModelCatalogServiceProvider;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;

class AlgorithmFacadeFactoryProvider {
    // dull utilities
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService = new AlgorithmMetaDataSetterService();
    private final FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();

    // Global state and services
    private final Log log;
    private final ConfigurationParser configurationParser;
    private final DefaultsConfiguration defaultsConfiguration;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final LimitsConfiguration limitsConfiguration;
    private final boolean useMaxMemoryEstimation;

    // Request scoped state and services
    private final AlgorithmMetricsService algorithmMetricsService;
    private final ModelCatalogServiceProvider modelCatalogServiceProvider;

    AlgorithmFacadeFactoryProvider(
        Log log,
        ConfigurationParser configurationParser,
        DefaultsConfiguration defaultsConfiguration,
        GraphStoreCatalogService graphStoreCatalogService,
        LimitsConfiguration limitsConfiguration,
        boolean useMaxMemoryEstimation,
        AlgorithmMetricsService algorithmMetricsService,
        ModelCatalogServiceProvider modelCatalogServiceProvider
    ) {
        this.log = log;
        this.configurationParser = configurationParser;
        this.defaultsConfiguration = defaultsConfiguration;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.limitsConfiguration = limitsConfiguration;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;

        this.algorithmMetricsService = algorithmMetricsService;
        this.modelCatalogServiceProvider = modelCatalogServiceProvider;
    }

    AlgorithmFacadeFactory createAlgorithmFacadeFactory(
        Context context,
        RequestScopedDependencies requestScopedDependencies,
        KernelTransaction kernelTransaction,
        GraphDatabaseService graphDatabaseService,
        DatabaseGraphStoreEstimationService databaseGraphStoreEstimationService
    ) {
        /*
         * GDS services derived from Procedure Context.
         * These come in layers, we can create some services readily,
         * but others need some of our own products and come later.
         * I have tried to mark those layers in comments below.
         */
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);
        var mutateNodePropertyService = new MutateNodePropertyService(log);
        var mutateRelationshipService = new MutateRelationshipService(log);
        var nodeLookup = new TransactionNodeLookup(kernelTransaction);
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());

        // Second layer
        var configurationCreator = new ConfigurationCreator(
            configurationParser,
            algorithmMetaDataSetter,
            requestScopedDependencies.getUser()
        );

        // Third layer
        var writeNodePropertyService = new WriteNodePropertyService(
            log,
            requestScopedDependencies.getNodePropertyExporterBuilder(),
            requestScopedDependencies.getTaskRegistryFactory(),
            requestScopedDependencies.getTerminationFlag()
        );
        var writeRelationshipService = new WriteRelationshipService(
            log,
            requestScopedDependencies.getRelationshipExporterBuilder(),
            requestScopedDependencies.getTaskRegistryFactory(),
            requestScopedDependencies.getTerminationFlag()
        );

        // Fourth layer
        var algorithmEstimator = new AlgorithmEstimator(
            graphStoreCatalogService,
            fictitiousGraphStoreEstimationService,
            databaseGraphStoreEstimationService,
            requestScopedDependencies
        );
        var algorithmRunner = new AlgorithmRunner(
            log,
            graphStoreCatalogService,
            algorithmMetricsService,
            algorithmMemoryValidationService,
            requestScopedDependencies,
            requestScopedDependencies.getTaskRegistryFactory(),
            requestScopedDependencies.getUserLogRegistryFactory()
        );

        // procedure facade
        return new AlgorithmFacadeFactory(
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
            requestScopedDependencies.getUser(),
            modelCatalogServiceProvider.createService(graphDatabaseService, log)
        );
    }
}
