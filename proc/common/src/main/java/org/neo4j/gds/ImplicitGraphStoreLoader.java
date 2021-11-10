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
package org.neo4j.gds;

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.transaction.TransactionContext;

public class ImplicitGraphStoreLoader implements GraphStoreLoader {

    private final GraphCreateConfig graphCreateConfig;
    private final String username;
    private final GraphLoaderContext graphLoaderContext;

    public static ImplicitGraphStoreLoader fromBaseProc(GraphCreateConfig graphCreateConfig, TaskRegistryFactory taskRegistryFactory, BaseProc baseProc) {
        var graphLoaderContext = ImmutableGraphLoaderContext.builder()
            .transactionContext(TransactionContext.of(baseProc.api, baseProc.procedureTransaction))
            .api(baseProc.api)
            .log(baseProc.log)
            .allocationTracker(baseProc.allocationTracker)
            .taskRegistryFactory(taskRegistryFactory)
            .terminationFlag(TerminationFlag.wrap(baseProc.transaction))
            .build();
        return new ImplicitGraphStoreLoader(
            graphCreateConfig,
            baseProc.username(),
            graphLoaderContext
        );
    }

    ImplicitGraphStoreLoader(
        GraphCreateConfig graphCreateConfig,
        String username,
        GraphLoaderContext graphLoaderContext
    ) {
        this.graphCreateConfig = graphCreateConfig;
        this.username = username;
        this.graphLoaderContext = graphLoaderContext;
    }

    @Override
    public GraphCreateConfig graphCreateConfig() {
        return this.graphCreateConfig;
    }

    @Override
    public GraphStore graphStore() {
        return newLoader().graphStore();
    }

    GraphLoader newLoader() {
        if (graphLoaderContext.api() == null) {
            return newFictitiousLoader(graphCreateConfig);
        }
        return ImmutableGraphLoader
            .builder()
            .context(graphLoaderContext)
            .username(username)
            .createConfig(graphCreateConfig)
            .build();
    }

    private GraphLoader newFictitiousLoader(GraphCreateConfig createConfig) {
        return ImmutableGraphLoader
            .builder()
            .context(GraphLoaderContext.NULL_CONTEXT)
            .username(username)
            .createConfig(createConfig)
            .build();
    }
}
