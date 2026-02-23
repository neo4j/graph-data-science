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
package org.neo4j.gds.procedures.integration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.EmptyTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreHolder;
import org.neo4j.gds.settings.GdsSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

/**
 * @deprecated We need this to _not_ rely on a static singleton for state sharing
 */
@Deprecated
@SuppressWarnings("unused")
@ServiceProvider
public final class TaskStoreAndRegistryExtension extends ExtensionFactory<TaskStoreAndRegistryExtension.Dependencies> {
    public TaskStoreAndRegistryExtension() {
        super(ExtensionType.DATABASE, "gds.task.store_and_registry");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        var registry = dependencies.globalProceduresRegistry();
        var enabled = dependencies.config().get(GdsSettings.progressTrackingEnabled());
        var retentionPeriod = dependencies.config().get(GdsSettings.taskRetentionPeriod());
        var databaseName = dependencies.graphDatabaseService().databaseName();

        if (enabled) {
            var taskStoreProvider = new OldTaskStoreProvider(retentionPeriod);
            // Use the centrally managed task stores
            registry.registerComponent(TaskStore.class, taskStoreProvider, true);
            registry.registerComponent(TaskRegistryFactory.class, new OldTaskRegistryFactoryProvider(taskStoreProvider), true);

            // hey this is just for tests? TaskRegistryExtensionMultiDBTest breaks if it is missing
            extensionContext.dependencySatisfier().satisfyDependency(TaskStoreHolder.getTaskStore(databaseName, retentionPeriod));
        } else {
            registry.registerComponent(TaskRegistryFactory.class, ctx -> EmptyTaskRegistryFactory.INSTANCE, true);
            registry.registerComponent(TaskStore.class, ctx -> EmptyTaskStore.INSTANCE, true);

            extensionContext.dependencySatisfier().satisfyDependency(EmptyTaskStore.INSTANCE);
        }

        /*
         * Here we remember to cleanse the shared state.
         * We need to retain this functionality when this extension goes away.
         * Or perhaps this extension needs to persist because how else to get notified on the database removed event?
         * It can still exist even if it stops registering components.
         * We want the _access_ to go via centralised management in Procedure Facade.
         * We shall solve this later.
         */
        return new LifecycleAdapter() {
            @Override
            public void shutdown() {
                TaskStoreHolder.purge(databaseName);
            }
        };
    }

    public interface Dependencies {
        Config config();

        LogService logService();

        GlobalProcedures globalProceduresRegistry();

        GraphDatabaseService graphDatabaseService();
    }
}
