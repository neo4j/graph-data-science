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
import org.neo4j.gds.api.User;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphProjectNativeResult;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ReverseLogAdapter;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.executor.FictitiousGraphStoreLoader;
import org.neo4j.gds.executor.GraphStoreCreator;
import org.neo4j.gds.executor.GraphStoreFromDatabaseLoader;
import org.neo4j.gds.executor.MemoryUsageValidator;
import org.neo4j.gds.logging.Log;
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
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectFromStoreConfig configuration
    ) {
        try {
            return projectGraph(
                databaseId,
                taskRegistryFactory, terminationFlag, transactionContext, user,
                userLogRegistryFactory,
                configuration
            );
        } catch (RuntimeException e) {
            log.warn("Graph creation failed", e);
            throw e;
        }
    }

    private GraphProjectNativeResult projectGraph(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectFromStoreConfig config
    ) {
        // do later
        memoryUsageValidator().tryValidateMemoryUsage(
            config,
            graphProjectFromStoreConfig -> memoryTreeWithDimensions(
                config,
                databaseId,
                terminationFlag,
                transactionContext,
                taskRegistryFactory,
                userLogRegistryFactory,
                user
            )
        );

        var builder = new GraphProjectNativeResult.Builder(config);

        try (ProgressTimer ignored = ProgressTimer.start(builder::withProjectMillis)) {
            var graphLoaderContext = graphLoaderContext(
                databaseId,
                taskRegistryFactory, terminationFlag, transactionContext,
                userLogRegistryFactory
            );
            var graphStore = new GraphStoreFromDatabaseLoader(
                config,
                config.username(),
                graphLoaderContext
            ).graphStore();

            builder
                .withNodeCount(graphStore.nodeCount())
                .withRelationshipCount(graphStore.relationshipCount());

            GraphStoreCatalog.set(config, graphStore);
        }

        return builder.build();
    }

    private MemoryUsageValidator memoryUsageValidator() {
        return new MemoryUsageValidator(
            new ReverseLogAdapter(log),
            GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService)
        );
    }

    private MemoryTreeWithDimensions memoryTreeWithDimensions(
        GraphProjectConfig config,
        DatabaseId databaseId,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        User user
    ) {
        GraphStoreCreator graphStoreCreator;
        if (config.isFictitiousLoading()) {
            graphStoreCreator = new FictitiousGraphStoreLoader(config);
        } else {
            graphStoreCreator = new GraphStoreFromDatabaseLoader(
                config,
                user.getUsername(),
                graphLoaderContext(
                    databaseId,
                    taskRegistryFactory, terminationFlag, transactionContext,
                    userLogRegistryFactory
                )
            );
        }
        var graphDimensions = graphStoreCreator.graphDimensions();

        MemoryTree memoryTree = graphStoreCreator
            .estimateMemoryUsageDuringLoading()
            .estimate(graphDimensions, config.readConcurrency());

        return new MemoryTreeWithDimensions(memoryTree, graphDimensions);
    }

    private GraphLoaderContext graphLoaderContext(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag, TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        return ImmutableGraphLoaderContext.builder()
            .databaseId(databaseId)
            .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService))
            .log(new ReverseLogAdapter(log))
            .taskRegistryFactory(taskRegistryFactory)
            .terminationFlag(terminationFlag)
            .transactionContext(transactionContext)
            .userLogRegistryFactory(userLogRegistryFactory)
            .build();
    }
}
