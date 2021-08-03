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
public final class ProgressEventConsumerExtension extends ExtensionFactory<ProgressEventConsumerExtension.Dependencies> {
    private final @Nullable JobScheduler scheduler;

    @SuppressWarnings("unused - entry point for service loader")
    public ProgressEventConsumerExtension() {
        this(Optional.empty());
    }

    @TestOnly
    public ProgressEventConsumerExtension(JobScheduler scheduler) {
        this(Optional.of(scheduler));
    }

    private ProgressEventConsumerExtension(Optional<JobScheduler> maybeScheduler) {
        super(ExtensionType.DATABASE, "gds.progress.logger");
        this.scheduler = maybeScheduler.orElse(null);
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, ProgressEventConsumerExtension.Dependencies dependencies) {
        var registry = dependencies.globalProceduresRegistry();
        var enabled = dependencies.config().get(ProgressFeatureSettings.progress_tracking_enabled);
        if (enabled) {
            var scheduler = Optional.ofNullable(this.scheduler).orElseGet(dependencies::jobScheduler);
            var progressEventConsumerComponent = new ProgressEventConsumerComponent(
                dependencies.logService().getInternalLog(ProgressEventConsumerComponent.class),
                scheduler,
                dependencies.globalMonitors()
            );
            registry.registerComponent(ProgressEventTracker.class, progressEventConsumerComponent, true);
            registry.registerComponent(
                ProgressEventStore.class,
                ctx -> progressEventConsumerComponent.progressEventConsumer(),
                true
            );
            return progressEventConsumerComponent;
        } else {
            registry.registerComponent(ProgressEventTracker.class, ctx -> EmptyProgressEventTracker.INSTANCE, true);
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
