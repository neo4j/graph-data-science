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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Optional;

public interface GraphStoreLoader {
    GraphCreateConfig graphCreateConfig();
    GraphStore graphStore();

    static GraphStoreLoader of(
        AlgoBaseConfig config,
        Optional<String> maybeGraphName,
        String username,
        NamedDatabaseId databaseId,
        boolean isGdsAdmin,
        AllocationTracker allocationTracker,
        TaskRegistryFactory taskRegistryFactory,
        GraphDatabaseAPI api,
        Transaction transaction,
        KernelTransaction kernelTransaction,
        Log log
    ) {
        if (maybeGraphName.isPresent()) {
            return new GraphStoreFromCatalogLoader(maybeGraphName.get(), config, username, databaseId, isGdsAdmin);
        } else if (config.implicitCreateConfig().isPresent()) {
            return new ImplicitGraphStoreLoader(
                config.implicitCreateConfig().get(),
                allocationTracker,
                taskRegistryFactory,
                api,
                transaction,
                kernelTransaction,
                log,
                username
            );
        } else {
            throw new IllegalStateException("There must be either a graph name or an implicit create config");
        }
    }
}
