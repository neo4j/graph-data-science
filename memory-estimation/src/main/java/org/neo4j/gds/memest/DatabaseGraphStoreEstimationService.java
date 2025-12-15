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
package org.neo4j.gds.memest;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphStoreFactorySuppliers;

public class DatabaseGraphStoreEstimationService {
    private final GraphLoaderContext graphLoaderContext;
    private final GraphStoreFactorySuppliers graphStoreFactorySuppliers;
    private final DependencyResolver dependencyResolver;

    public DatabaseGraphStoreEstimationService(
        GraphLoaderContext graphLoaderContext,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        DependencyResolver dependencyResolver
    ) {
        this.graphLoaderContext = graphLoaderContext;
        this.graphStoreFactorySuppliers = graphStoreFactorySuppliers;
        this.dependencyResolver = dependencyResolver;
    }

    public GraphMemoryEstimation estimate(GraphProjectConfig graphProjectConfig) {
        var graphStoreFactory = graphStoreFactorySuppliers.find(graphProjectConfig).get(graphLoaderContext, dependencyResolver);

        return new GraphMemoryEstimation(
            graphStoreFactory.dimensions(),
            graphStoreFactory.estimateMemoryUsageAfterLoading()
        );
    }
}
