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
package org.neo4j.gds.core.utils.progress;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

/**
 * We need to make sure that state here is shared with Procedure Facade.
 * Procedure Facade needs the ability to be new'ed up with clean state.
 * Therefore, it needs to be able to drive the shared state.
 * At the same time, for tests, this extension can exist without Procedure Facade,
 * and in that situation needs to be able to drive state.
 * Given this analysis: we must keep the shared state somewhere where it is accessible from both this extension,
 * and from Procedure Facade.
 *
 * @deprecated remove this when usages have been strangled in favour of Procedure Facade
 */
@Deprecated
@ServiceProvider
public final class TaskRegistryExtension extends ExtensionFactory<TaskRegistryExtension.Dependencies> {

    public TaskRegistryExtension() {
        super(ExtensionType.DATABASE, "gds.task.registry");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, TaskRegistryExtension.Dependencies dependencies) {
        var registry = dependencies.globalProceduresRegistry();
        var enabled = dependencies.config().get(ProgressFeatureSettings.progress_tracking_enabled);
        String databaseName = dependencies.graphDatabaseService().databaseName();
        if (enabled) {
            // Use the centrally managed task stores
            GlobalTaskStore globalTaskStore = TaskStoreHolder.getTaskStore(databaseName);

            registry.registerComponent(TaskStore.class, ctx -> globalTaskStore, true);
            registry.registerComponent(TaskRegistryFactory.class, globalTaskStore, true);

            // hey this is just for tests? TaskRegistryExtensionMultiDBTest breaks if it is missing
            context.dependencySatisfier().satisfyDependency(globalTaskStore);
        } else {
            registry.registerComponent(TaskRegistryFactory.class, ctx -> EmptyTaskRegistryFactory.INSTANCE, true);
            registry.registerComponent(TaskStore.class, ctx -> EmptyTaskStore.INSTANCE, true);
            context.dependencySatisfier().satisfyDependency(EmptyTaskRegistryFactory.INSTANCE);
            context.dependencySatisfier().satisfyDependency(EmptyTaskStore.INSTANCE);
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
            public void shutdown() throws Exception {
                TaskStoreHolder.purge(databaseName);
            }
        };
    }

    interface Dependencies {
        Config config();

        LogService logService();

        GlobalProcedures globalProceduresRegistry();

        GraphDatabaseService graphDatabaseService();
    }
}
