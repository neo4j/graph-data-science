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
package org.neo4j.gds.internal;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

@ServiceProvider
public final class AuraMaintenanceExtension extends ExtensionFactory<AuraMaintenanceExtension.Dependencies> {

    private final boolean blockOnRestore;

    @SuppressWarnings("unused - entry point for service loader")
    public AuraMaintenanceExtension() {
        this(false);
    }

    public AuraMaintenanceExtension(boolean blockOnRestore) {
        super(ExtensionType.GLOBAL, "gds.aura.maintenance");
        this.blockOnRestore = blockOnRestore;
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, AuraMaintenanceExtension.Dependencies dependencies) {
        var enabled = dependencies.config().get(AuraMaintenanceSettings.maintenance_function_enabled);
        if (enabled) {
            var exportLocationSetting = dependencies.config().get(GraphStoreExportSettings.export_location_setting);
            if (exportLocationSetting == null) {
                dependencies.logService()
                    .getInternalLog(getClass())
                    .warn(
                        "The configuration %s is missing, no restore from the export location will be attempted.",
                        GraphStoreExportSettings.export_location_setting.name()
                    );
            }

            var registry = dependencies.globalProceduresRegistry();
            try {
                registry.register(new AuraMaintenanceFunction(), false);
                registry.register(new AuraShutdownProc(), false);
            } catch (ProcedureException e) {
                dependencies.logService()
                    .getInternalLog(getClass())
                    .warn(e.getMessage(), e);
            }

            if (exportLocationSetting != null) {
                return LifecycleAdapter.onInit(() -> {
                    var jobScheduler = dependencies.jobScheduler();
                    var jobHandle = jobScheduler.schedule(
                        Group.FILE_IO_HELPER,
                        () -> restorePersistedData(
                            dependencies.config(),
                            dependencies.logService()
                        )
                    );
                    if (blockOnRestore) {
                        jobHandle.waitTermination();
                    }
                });
            }
        }
        return new LifecycleAdapter();
    }

    private static void restorePersistedData(Configuration neo4jConfig, LogService logService) {
        var userLog = logService.getUserLog(AuraMaintenanceExtension.class);
        var importDir = neo4jConfig.get(GraphStoreExportSettings.export_location_setting);
        try {
            BackupAndRestore.restore(importDir, userLog);
        } catch (Exception e) {
            userLog.warn("Graph store loading failed", e);
        }
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProceduresRegistry();

        LogService logService();

        JobScheduler jobScheduler();
    }
}
