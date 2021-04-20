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
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@ServiceProvider
public final class AuraRestoreExtension extends ExtensionFactory<AuraRestoreExtension.Dependencies> {

    public AuraRestoreExtension() {
        super(ExtensionType.GLOBAL, "gds.aura.restore");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void init() {
                var userLog = dependencies
                    .logService()
                    .getUserLog(getClass());

                var storePath = dependencies.config().get(GraphStoreExportSettings.export_location_setting);
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

            @Override
            public void shutdown() {
            }
        };
    }

    private Stream<Path> getImportPaths(Path storePath) throws IOException {
        return Files.list(storePath)
            .peek(CsvToGraphStoreExporter.DIRECTORY_IS_READABLE::validate)
            .filter(graphDir -> Files.exists(graphDir.resolve(AutoloadFlagVisitor.AUTOLOAD_FILE_NAME)));
    }

    interface Dependencies {
        Config config();

        LogService logService();
    }
}
