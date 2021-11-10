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
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

public class ImplicitGraphStoreLoader implements GraphStoreLoader {

    private final GraphCreateConfig graphCreateConfig;
    private final AllocationTracker allocationTracker;
    private final TaskRegistryFactory taskRegistryFactory;
    private final GraphDatabaseAPI api;
    private final Transaction transaction;
    private final KernelTransaction kernelTransaction;
    private final Log log;
    private final String username;

    public static ImplicitGraphStoreLoader fromBaseProc(GraphCreateConfig graphCreateConfig, TaskRegistryFactory taskRegistryFactory, BaseProc baseProc) {
        return new ImplicitGraphStoreLoader(
            graphCreateConfig,
            baseProc.allocationTracker(),
            taskRegistryFactory,
            baseProc.api,
            baseProc.procedureTransaction,
            baseProc.transaction,
            baseProc.log,
            baseProc.username()
        );
    }

    ImplicitGraphStoreLoader(
        GraphCreateConfig graphCreateConfig,
        AllocationTracker allocationTracker,
        TaskRegistryFactory taskRegistryFactory,
        GraphDatabaseAPI api,
        Transaction transaction,
        KernelTransaction kernelTransaction,
        Log log,
        String username
    ) {
        this.graphCreateConfig = graphCreateConfig;
        this.allocationTracker = allocationTracker;
        this.taskRegistryFactory = taskRegistryFactory;
        this.api = api;
        this.transaction = transaction;
        this.kernelTransaction = kernelTransaction;
        this.log = log;
        this.username = username;
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
        if (api == null) {
            return newFictitiousLoader(graphCreateConfig);
        }
        return ImmutableGraphLoader
            .builder()
            .context(ImmutableGraphLoaderContext.builder()
                .transactionContext(TransactionContext.of(api, transaction))
                .api(api)
                .log(log)
                .allocationTracker(allocationTracker)
                .taskRegistryFactory(taskRegistryFactory)
                .terminationFlag(TerminationFlag.wrap(kernelTransaction))
                .build())
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
