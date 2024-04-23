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

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.mem.MemoryEstimation;

public class GraphStoreFromDatabaseLoader implements GraphStoreCreator {

    private final GraphProjectConfig graphProjectConfig;
    private final String username;
    private final GraphLoaderContext graphLoaderContext;
    private final GraphStoreFactory<?, ?> graphStoreFactory;

    public GraphStoreFromDatabaseLoader(
        GraphProjectConfig graphProjectConfig,
        String username,
        GraphLoaderContext graphLoaderContext
    ) {
        this.graphProjectConfig = graphProjectConfig;
        this.username = username;
        this.graphLoaderContext = graphLoaderContext;
        this.graphStoreFactory = graphStoreFactory();
    }

    @Override
    public GraphProjectConfig graphProjectConfig() {
        return this.graphProjectConfig;
    }

    @Override
    public GraphStore graphStore() {
        return this.graphStoreFactory.build();
    }

    @Override
    public GraphDimensions graphDimensions() {
        return graphStoreFactory.estimationDimensions();
    }

    @Override
    public MemoryEstimation estimateMemoryUsageDuringLoading() {
        return this.graphStoreFactory.estimateMemoryUsageDuringLoading();
    }

    @Override
    public MemoryEstimation estimateMemoryUsageAfterLoading() {
        return this.graphStoreFactory.estimateMemoryUsageDuringLoading();
    }

    private GraphStoreFactory<?, ?> graphStoreFactory() {
        return ImmutableGraphLoader
            .builder()
            .context(graphLoaderContext)
            .username(username)
            .projectConfig(graphProjectConfig)
            .build()
            .graphStoreFactory();
    }
}
