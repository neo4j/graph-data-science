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
package org.neo4j.gds.extensions.shared;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.procedures.integration.LogAccessor;
import org.neo4j.gds.procedures.integration.TaskStoreObserver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

/**
 * We need this extension because we need the database bootstrap and shutdown events.
 * And because our testing relies on registering task stores in Neo4j. We really should fix that bit some time.
 */
@ServiceProvider
public final class TaskStoreExtension extends ExtensionFactory<TaskStoreExtension.Dependencies> {
    private final LogAccessor logAccessor = new LogAccessor();

    public TaskStoreExtension() {
        super(ExtensionType.DATABASE, "gds.taskstore");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        var log = logAccessor.getLog(dependencies.logService(), getClass());
        // FIXME: Find a way to use the DatabaseIdSupplier
        var databaseId = DatabaseId.of(dependencies.graphDatabaseService().databaseName());
        log.info("Bootstrapping task store for new database '%s'", databaseId);

        // get a handle on the observer
        var taskStoreObserver = dependencies.graphDatabaseApi()
            .getDependencyResolver()
            .resolveDependency(TaskStoreObserver.class);

        // notify the observer that a new database has appeared, get a task store in return
        var taskStore = taskStoreObserver.onBootstrap(databaseId);

        // because of the way our testing works, we have to do this abominable detour :facepalm:
        // funnily enough that won't work for testing composite databases.
        // Because there the database name we get here in the extension,
        // is not the same database name that used to resolve stuff later. C'est la vie in this project.
        extensionContext.dependencySatisfier().satisfyDependency(taskStore);

        return new LifecycleAdapter() {
            @Override
            public void shutdown() {
                log.info("Purging task store for database '%s' because it is shutting down", databaseId);
                // notify the observer that the task store is no longer needed
                taskStoreObserver.onShutdown(databaseId);
            }
        };
    }

    public interface Dependencies {
        GraphDatabaseAPI graphDatabaseApi();

        GraphDatabaseService graphDatabaseService();

        LogService logService();
    }
}
