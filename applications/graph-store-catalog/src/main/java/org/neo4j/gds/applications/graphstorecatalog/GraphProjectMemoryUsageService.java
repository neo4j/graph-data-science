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

import org.neo4j.configuration.Config;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;
import org.neo4j.gds.settings.GdsSettings;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

public class GraphProjectMemoryUsageService {
    private final Log log;
    private final GraphDatabaseService graphDatabaseService;
    private final GraphStoreFactorySuppliers graphStoreFactorySuppliers;
    private final MemoryTracker memoryTracker;

    public GraphProjectMemoryUsageService(
        Log log,
        GraphDatabaseService graphDatabaseService,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        MemoryTracker memoryTracker
    ) {
        this.log = log;
        this.graphDatabaseService = graphDatabaseService;
        this.graphStoreFactorySuppliers = graphStoreFactorySuppliers;
        this.memoryTracker = memoryTracker;
    }

    void validateMemoryUsage(
        RequestScopedDependencies requestScopedDependencies,
        TransactionContext transactionContext,
        GraphProjectConfig configuration
    ) {
        memoryUsageValidator(requestScopedDependencies.user()).tryValidateMemoryUsage(
            "Loading",
            configuration,
            graphProjectConfig -> getEstimate(requestScopedDependencies, transactionContext, graphProjectConfig)
        );
    }

    MemoryTreeWithDimensions getEstimate(
        RequestScopedDependencies requestScopedDependencies,
        TransactionContext transactionContext,
        GraphProjectConfig configuration
    ) {
        var dependencyResolver = GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService);

        var graphLoaderContext = graphLoaderContext(requestScopedDependencies, transactionContext);

        var graphStoreFactorySupplier = graphStoreFactorySuppliers.find(configuration);
        var graphStoreFactory = graphStoreFactorySupplier.get(graphLoaderContext, dependencyResolver);

        var graphStoreCreator = new GraphStoreFromDatabaseLoader(configuration, graphStoreFactory);

        return computeEstimate(configuration, graphStoreCreator);
    }

    MemoryTreeWithDimensions getFictitiousEstimate(GraphProjectConfig configuration) {
        var graphStoreFactorySupplier = graphStoreFactorySuppliers.find(configuration);

        var graphStoreCreator = new FictitiousGraphStoreLoader(configuration, graphStoreFactorySupplier);

        return computeEstimate(configuration, graphStoreCreator);
    }

    private MemoryUsageValidator memoryUsageValidator(User user) {
        var neo4jConfig = GraphDatabaseApiProxy.dependencyResolver(graphDatabaseService)
            .resolveDependency(Config.class);
        var useMaxMemoryEstimation = neo4jConfig.get(GdsSettings.validateUsingMaxMemoryEstimation());

        return new MemoryUsageValidator(user.getUsername(), memoryTracker, useMaxMemoryEstimation, log);
    }

    private GraphLoaderContext graphLoaderContext(
        RequestScopedDependencies requestScopedDependencies,
        TransactionContext transactionContext
    ) {
        return ImmutableGraphLoaderContext.builder()
            .databaseId(requestScopedDependencies.databaseId())
            .log(log)
            .taskRegistryFactory(requestScopedDependencies.taskRegistryFactory())
            .terminationFlag(requestScopedDependencies.terminationFlag())
            .transactionContext(transactionContext)
            .userLogRegistry(requestScopedDependencies.userLogRegistry())
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
