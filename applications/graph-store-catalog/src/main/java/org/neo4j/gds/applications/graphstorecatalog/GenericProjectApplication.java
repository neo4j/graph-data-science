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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResultFactory;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphProjectResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.function.Function;

/**
 * Turns out native project and cypher project are identical modulo configurations,
 * because configurations provide functional elements (services).
 * I'm not a fan, nor actually a fan of generics, but here we achieve some good, DRY code.
 */
public class GenericProjectApplication<RESULT extends GraphProjectResult, CONFIGURATION extends GraphProjectConfig, RESULT_BUILDER extends GraphProjectResult.Builder<RESULT>> {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final GraphStoreFactorySuppliers graphStoreFactorySuppliers;
    private final Function<CONFIGURATION, RESULT_BUILDER> resultBuilderFactory;

    GenericProjectApplication(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        Function<CONFIGURATION, RESULT_BUILDER> resultBuilderFactory
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.graphStoreFactorySuppliers = graphStoreFactorySuppliers;
        this.resultBuilderFactory = resultBuilderFactory;
    }

    public RESULT project(
        DatabaseId databaseId,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistry userLogRegistry,
        CONFIGURATION configuration
    ) {
        try {
            return projectGraph(
                databaseId,
                graphDatabaseService,
                graphProjectMemoryUsageService,
                taskRegistryFactory,
                terminationFlag,
                transactionContext,
                userLogRegistry,
                configuration
            );
        } catch (RuntimeException e) {
            log.warn("Graph creation failed", e);
            throw e;
        }
    }

    public MemoryEstimateResult estimate(
        DatabaseId databaseId,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistry userLogRegistry,
        GraphProjectConfig configuration
    ) {
        if (configuration.isFictitiousLoading()) return estimateButFictitiously(
            graphProjectMemoryUsageService,
            configuration
        );

        var memoryTreeWithDimensions = graphProjectMemoryUsageService.getEstimate(
            databaseId,
            terminationFlag,
            transactionContext,
            taskRegistryFactory,
            userLogRegistry,
            configuration
        );

        return MemoryEstimateResultFactory.from(memoryTreeWithDimensions);
    }

    private RESULT projectGraph(
        DatabaseId databaseId,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistry userLogRegistry,
        CONFIGURATION configuration
    ) {
        graphProjectMemoryUsageService.validateMemoryUsage(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistry,
            configuration
        );

        RESULT_BUILDER resultBuilder = resultBuilderFactory.apply(configuration);

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withProjectMillis)) {
            var dependencyResolver = GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService);

            var graphLoaderContext = graphLoaderContext(
                databaseId,
                taskRegistryFactory,
                terminationFlag,
                transactionContext,
                userLogRegistry
            );
            var graphStoreFactorySupplier = graphStoreFactorySuppliers.find(configuration);
            var graphStoreFactory = graphStoreFactorySupplier.get(graphLoaderContext, dependencyResolver);
            var graphStoreCreator = new GraphStoreFromDatabaseLoader(
                configuration,
                graphStoreFactory
            );
            var graphStore = graphStoreCreator.graphStore();

            resultBuilder
                .withNodeCount(graphStore.nodeCount())
                .withRelationshipCount(graphStore.relationshipCount());

            graphStoreCatalogService.set(configuration, graphStore);
        }

        return resultBuilder.build();
    }

    /**
     * Public because EstimationCLI tests needs it. Should redesign something here I think
     */
    private MemoryEstimateResult estimateButFictitiously(
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        GraphProjectConfig configuration
    ) {
        var estimate = graphProjectMemoryUsageService.getFictitiousEstimate(configuration);

        return MemoryEstimateResultFactory.from(estimate);
    }

    private GraphLoaderContext graphLoaderContext(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistry userLogRegistry
    ) {
        return ImmutableGraphLoaderContext.builder()
            .databaseId(databaseId)
            .log(log)
            .taskRegistryFactory(taskRegistryFactory)
            .terminationFlag(terminationFlag)
            .transactionContext(transactionContext)
            .userLogRegistry(userLogRegistry)
            .build();
    }
}
