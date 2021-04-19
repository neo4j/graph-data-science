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
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.export.file.CsvToGraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.export.file.csv.AutoloadFlagVisitor;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@ServiceProvider
public final class AuraMaintenanceExtension extends ExtensionFactory<AuraMaintenanceExtension.Dependencies> {

    @SuppressWarnings("unused - entry point for service loader")
    public AuraMaintenanceExtension() {
        super(ExtensionType.GLOBAL, "gds.aura.maintenance");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, AuraMaintenanceExtension.Dependencies dependencies) {
        var enabled = dependencies.config().get(AuraMaintenanceSettings.maintenance_function_enabled);
        if (enabled) {
            var registry = dependencies.globalProceduresRegistry();
            try {
                registry.register(new AuraMaintenanceFunction(), false);
                registry.register(new AuraShutdownProc(), false);
                return LifecycleAdapter.onInit(() -> {
                    var jobScheduler = dependencies.jobScheduler();
                    jobScheduler.schedule(
                        Group.FILE_IO_HELPER,
                        () -> restorePersistedGraphs(
                            dependencies.config(),
                            dependencies.logService()
                        )
                    );
                });
            } catch (ProcedureException e) {
                dependencies.logService()
                    .getInternalLog(getClass())
                    .warn("Could not register aura maintenance function: " + e.getMessage(), e);
            }
        }
        return new LifecycleAdapter();
    }

    private static void restorePersistedGraphs(Configuration neo4jConfig, LogService logService) {
        var userLog = logService.getUserLog(AuraMaintenanceExtension.class);
        var storePath = neo4jConfig.get(GraphStoreExportSettings.export_location_setting);
        try {
            getImportPaths(storePath).forEach(path -> {
                var config = ImmutableGraphStoreToFileExporterConfig.builder()
                    .exportName("")
                    .includeMetaData(true)
                    .build();
                var graphStoreImporter = CsvToGraphStoreExporter.create(config, path);

                graphStoreImporter.run(AllocationTracker.empty());

                var graphStore = graphStoreImporter.userGraphStore();

                var graphName = path.getFileName().toString();
                var createConfig = GraphCreateFromStoreConfig.emptyWithName(
                    graphStore.userName(),
                    graphName
                );
                GraphStoreCatalog.set(createConfig, graphStore.graphStore());
            });
        } catch (Exception e) {
            userLog.warn("Graph store loading failed", e);
        }
    }

    private static Stream<Path> getImportPaths(Path storePath) throws IOException {
        return Files.list(storePath)
            .peek(CsvToGraphStoreExporter.DIRECTORY_IS_READABLE::validate)
            .filter(graphDir -> Files.exists(graphDir.resolve(AutoloadFlagVisitor.AUTOLOAD_FILE_NAME)));
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProceduresRegistry();

        LogService logService();

        JobScheduler jobScheduler();
    }
}
