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
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

public class GraphProjectMemoryUsageService {
    private final Log log;
    private final GraphDatabaseService graphDatabaseService;

    public GraphProjectMemoryUsageService(Log log, GraphDatabaseService graphDatabaseService) {
        this.log = log;
        this.graphDatabaseService = graphDatabaseService;
    }

    public void validateMemoryUsage(
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectConfig configuration
    ) {
        memoryUsageValidator().tryValidateMemoryUsage(
            configuration,
            graphProjectConfig -> getEstimate(
                databaseId,
                terminationFlag,
                transactionContext,
                taskRegistryFactory,
                userLogRegistryFactory,
                graphProjectConfig
            )
        );
    }

    public MemoryTreeWithDimensions getEstimate(
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

        return computeEstimate(configuration, graphStoreCreator);
    }

    public MemoryTreeWithDimensions getFictitiousEstimate(GraphProjectConfig configuration) {
        var graphStoreCreator = new FictitiousGraphStoreLoader(configuration);

        return computeEstimate(configuration, graphStoreCreator);
    }

    private MemoryUsageValidator memoryUsageValidator() {
        return new MemoryUsageValidator(
            log,
            GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService)
        );
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
            .log(log)
            .taskRegistryFactory(taskRegistryFactory)
            .terminationFlag(terminationFlag)
            .transactionContext(transactionContext)
            .userLogRegistryFactory(userLogRegistryFactory)
            .build();
    }

    private static MemoryTreeWithDimensions computeEstimate(
        GraphProjectConfig config,
        GraphStoreCreator graphStoreCreator
    ) {
        var graphDimensions = graphStoreCreator.graphDimensions();

        var memoryTree = graphStoreCreator
            .estimateMemoryUsageDuringLoading()
            .estimate(graphDimensions, config.readConcurrency());

        return new MemoryTreeWithDimensions(memoryTree, graphDimensions);
    }
}
