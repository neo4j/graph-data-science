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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import java.util.Optional;

@ServiceProvider
public final class ProgressEventExtension extends ExtensionFactory<ProgressEventExtension.Dependencies> {
    private final @Nullable JobScheduler scheduler;

    @SuppressWarnings("unused - entry point for service loader")
    public ProgressEventExtension() {
        this(Optional.empty());
    }

    @TestOnly
    public ProgressEventExtension(JobScheduler scheduler) {
        this(Optional.of(scheduler));
    }

    private ProgressEventExtension(Optional<JobScheduler> maybeScheduler) {
        super(ExtensionType.DATABASE, "gds.progress.logger");
        this.scheduler = maybeScheduler.orElse(null);
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, ProgressEventExtension.Dependencies dependencies) {
        var registry = dependencies.globalProceduresRegistry();
        var enabled = dependencies.config().get(ProgressFeatureSettings.progress_tracking_enabled);
        if (enabled) {
            var globalTaskRegistry = new GlobalTaskRegistryMap();
            registry.registerComponent(TaskStore.class, ctx -> globalTaskRegistry, true);
            registry.registerComponent(TaskRegistry.class, globalTaskRegistry, true);
            var scheduler = Optional.ofNullable(this.scheduler).orElseGet(dependencies::jobScheduler);
            var progressEventConsumerComponent = new ProgressEventComponent(
                dependencies.logService().getInternalLog(ProgressEventComponent.class),
                scheduler,
                dependencies.globalMonitors()
            );
            registry.registerComponent(
                ProgressEventStore.class,
                ctx -> progressEventConsumerComponent.progressEventStore(),
                true
            );
            return progressEventConsumerComponent;
        } else {
            registry.registerComponent(TaskRegistry.class, ctx -> EmptyTaskRegistry.INSTANCE, true);
            registry.registerComponent(TaskStore.class, ctx -> EmptyTaskStore.INSTANCE, true);
            registry.registerComponent(ProgressEventStore.class, ctx -> EmptyProgressEventStore.INSTANCE, true);
            return new LifecycleAdapter();
        }
    }

    interface Dependencies {
        Config config();

        LogService logService();

        JobScheduler jobScheduler();

        Monitors globalMonitors();

        GlobalProcedures globalProceduresRegistry();
    }
}
