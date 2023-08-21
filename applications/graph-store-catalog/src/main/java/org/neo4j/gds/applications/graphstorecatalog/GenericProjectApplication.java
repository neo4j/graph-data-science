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
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphProjectResult;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.executor.GraphStoreFromDatabaseLoader;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.results.MemoryEstimateResult;
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
    private final GraphDatabaseService graphDatabaseService;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final GraphProjectMemoryUsageService graphProjectMemoryUsageService;

    private final Function<CONFIGURATION, RESULT_BUILDER> resultBuilderFactory;

    public GenericProjectApplication(
        Log log,
        GraphDatabaseService graphDatabaseService,
        GraphStoreCatalogService graphStoreCatalogService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        Function<CONFIGURATION, RESULT_BUILDER> resultBuilderFactory
    ) {
        this.log = log;
        this.graphDatabaseService = graphDatabaseService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.graphProjectMemoryUsageService = graphProjectMemoryUsageService;
        this.resultBuilderFactory = resultBuilderFactory;
    }

    public RESULT project(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        CONFIGURATION configuration
    ) {
        try {
            return projectGraph(
                databaseId,
                taskRegistryFactory,
                terminationFlag,
                transactionContext,
                userLogRegistryFactory,
                configuration
            );
        } catch (RuntimeException e) {
            log.warn("Graph creation failed", e);
            throw e;
        }
    }

    public MemoryEstimateResult estimate(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectConfig configuration
    ) {
        if (configuration.isFictitiousLoading()) return estimateButFictitiously(configuration);

        var memoryTreeWithDimensions = graphProjectMemoryUsageService.getEstimate(
            databaseId,
            terminationFlag,
            transactionContext,
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration
        );

        return new MemoryEstimateResult(memoryTreeWithDimensions);
    }

    private RESULT projectGraph(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        CONFIGURATION configuration
    ) {
        graphProjectMemoryUsageService.validateMemoryUsage(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );

        RESULT_BUILDER resultBuilder = resultBuilderFactory.apply(configuration);

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withProjectMillis)) {
            var graphLoaderContext = graphLoaderContext(
                databaseId,
                taskRegistryFactory,
                terminationFlag,
                transactionContext,
                userLogRegistryFactory
            );
            var graphStore = new GraphStoreFromDatabaseLoader(
                configuration,
                configuration.username(),
                graphLoaderContext
            ).graphStore();

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
    public MemoryEstimateResult estimateButFictitiously(GraphProjectConfig configuration) {
        var estimate = graphProjectMemoryUsageService.getFictitiousEstimate(configuration);

        return new MemoryEstimateResult(estimate);
    }

    private GraphLoaderContext graphLoaderContext(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        return ImmutableGraphLoaderContext.builder()
            .databaseId(databaseId)
            .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService))
            .log((org.neo4j.logging.Log) log.getNeo4jLog())
            .taskRegistryFactory(taskRegistryFactory)
            .terminationFlag(terminationFlag)
            .transactionContext(transactionContext)
            .userLogRegistryFactory(userLogRegistryFactory)
            .build();
    }
}
