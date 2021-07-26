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

import org.jetbrains.annotations.Nullable;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import java.nio.file.Path;

@ServiceProvider
public final class AuraMaintenanceExtension extends ExtensionFactory<AuraMaintenanceExtension.Dependencies> {

    private static final String RESTORE_DIR = "restore";
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
            var config = dependencies.config();
            var log = dependencies.logService().getInternalLog(getClass());

            var backupPath = pathSetting(config, GraphStoreExportSettings.backup_location_setting, log);
            var exportPath = pathSetting(config, GraphStoreExportSettings.export_location_setting, log);
            // We keep it null here so that we can fail lazily from the shutdown proc with a message
            // that we control and is visible to the user outside of going through the logs
            var restorePath = exportPath != null ? exportPath.resolve(RESTORE_DIR) : null;

            var registry = dependencies.globalProceduresRegistry();
            try {
                registry.register(new AuraMaintenanceFunction(), false);
                registry.register(new AuraShutdownProc(restorePath), false);
            } catch (ProcedureException e) {
                log.warn(e.getMessage(), e);
            }

            if (restorePath != null && backupPath != null) {
                return LifecycleAdapter.onInit(() -> {
                    var jobScheduler = dependencies.jobScheduler();
                    var jobHandle = jobScheduler.schedule(
                        Group.FILE_IO_HELPER,
                        () -> restorePersistedData(
                            dependencies.logService(),
                            restorePath,
                            backupPath
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

    private static void restorePersistedData(
        LogService logService,
        Path restorePath,
        Path backupPath
    ) {
        var userLog = logService.getUserLog(AuraMaintenanceExtension.class);
        try {
            BackupAndRestore.restore(restorePath, backupPath, userLog);
        } catch (Exception e) {
            userLog.warn("Graph store loading failed", e);
        }
    }

    private static @Nullable Path pathSetting(Configuration config, Setting<Path> setting, Log log) {
        var path = config.get(setting);
        if (path == null) {
            log.warn(
                "The configuration %s is missing, no restore from the export location will be attempted.",
                setting.name()
            );
        }
        return path;
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProceduresRegistry();

        LogService logService();

        JobScheduler jobScheduler();
    }
}
