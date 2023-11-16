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

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.ImmutableGraphLoader;

public class DatabaseGraphStoreEstimationService {
    private final GraphLoaderContext graphLoaderContext;
    private final User user;

    public DatabaseGraphStoreEstimationService(GraphLoaderContext graphLoaderContext, User user) {
        this.graphLoaderContext = graphLoaderContext;
        this.user = user;
    }

    public GraphMemoryEstimation estimate(GraphProjectConfig graphProjectConfig) {
        var graphStoreFactory = ImmutableGraphLoader
            .builder()
            .context(this.graphLoaderContext)
            .username(this.user.getUsername())
            .projectConfig(graphProjectConfig)
            .build()
            .graphStoreFactory();

        return new GraphMemoryEstimation(
            graphStoreFactory.dimensions(),
            graphStoreFactory.estimateMemoryUsageAfterLoading()
        );
    }
}
