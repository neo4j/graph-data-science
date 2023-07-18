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
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphProjectNativeResult;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.executor.FictitiousGraphStoreLoader;
import org.neo4j.gds.executor.GraphStoreCreator;
import org.neo4j.gds.executor.GraphStoreFromDatabaseLoader;
import org.neo4j.gds.executor.MemoryUsageValidator;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

public class NativeProjectService {
    private final Log log;
    private final GraphDatabaseService graphDatabaseService;

    public NativeProjectService(Log log, GraphDatabaseService graphDatabaseService) {
        this.log = log;
        this.graphDatabaseService = graphDatabaseService;
    }

    public GraphProjectNativeResult compute(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectFromStoreConfig configuration
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

    public MemoryEstimateResult estimateButFictitiously(GraphProjectFromStoreConfig configuration) {
        var memoryTreeWithDimensions = fictitiousMemoryTreeWithDimensions(configuration);

        return new MemoryEstimateResult(memoryTreeWithDimensions);
    }

    public MemoryEstimateResult estimate(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectConfig configuration
    ) {
        var memoryTreeWithDimensions = computeMemoryTreeWithDimensions(
            databaseId,
            terminationFlag,
            transactionContext,
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration
        );

        return new MemoryEstimateResult(memoryTreeWithDimensions);
    }

    private GraphProjectNativeResult projectGraph(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectFromStoreConfig configuration
    ) {
        // do later
        memoryUsageValidator().tryValidateMemoryUsage(
            configuration,
            graphProjectConfig -> computeMemoryTreeWithDimensions(
                databaseId,
                terminationFlag,
                transactionContext,
                taskRegistryFactory,
                userLogRegistryFactory,
                graphProjectConfig
            )
        );

        var builder = new GraphProjectNativeResult.Builder(configuration);

        try (ProgressTimer ignored = ProgressTimer.start(builder::withProjectMillis)) {
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

            builder
                .withNodeCount(graphStore.nodeCount())
                .withRelationshipCount(graphStore.relationshipCount());

            GraphStoreCatalog.set(configuration, graphStore);
        }

        return builder.build();
    }

    private MemoryUsageValidator memoryUsageValidator() {
        return new MemoryUsageValidator(
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService)
        );
    }

    private MemoryTreeWithDimensions computeMemoryTreeWithDimensions(
        DatabaseId databaseId,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectConfig configuration
    ) {
        var graphLoaderContext = graphLoaderContext(
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory
        );

        var graphStoreCreator = new GraphStoreFromDatabaseLoader(
            configuration,
            "unused", // again, too wide types
            graphLoaderContext
        );

        return computeMemoryTreeWithDimensions(configuration, graphStoreCreator);
    }

    private MemoryTreeWithDimensions fictitiousMemoryTreeWithDimensions(GraphProjectConfig config) {
        var graphStoreCreator = new FictitiousGraphStoreLoader(config);

        return computeMemoryTreeWithDimensions(config, graphStoreCreator);
    }

    private static MemoryTreeWithDimensions computeMemoryTreeWithDimensions(
        GraphProjectConfig config,
        GraphStoreCreator graphStoreCreator
    ) {
        var graphDimensions = graphStoreCreator.graphDimensions();

        var memoryTree = graphStoreCreator
            .estimateMemoryUsageDuringLoading()
            .estimate(graphDimensions, config.readConcurrency());

        return new MemoryTreeWithDimensions(memoryTree, graphDimensions);
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
