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
package org.neo4j.gds.core.io.file;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphStoreExportSettings;
import org.neo4j.gds.core.io.GraphStoreExporter;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.file.csv.GraphStoreToCsvExporter;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.neo4j.gds.core.io.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GraphStoreExporterUtil {

    public static final String EXPORT_DIR = "export";

    public static ExportToCsvResult export(
        GraphStore graphStore,
        Path path,
        GraphStoreToFileExporterConfig config,
        Optional<NeoNodeProperties> neoNodeProperties,
        TaskRegistryFactory taskRegistryFactory,
        Log log
    ) {
        try {
            var exporter = GraphStoreToCsvExporter.create(graphStore, config, path, neoNodeProperties, taskRegistryFactory, log);

            var start = System.nanoTime();
            var exportedProperties = exporter.run();
            var end = System.nanoTime();

            var tookMillis = TimeUnit.NANOSECONDS.toMillis(end - start);
            log.info("[gds] Export completed for '%s' in %s ms", config.exportName(), tookMillis);
            return ImmutableExportToCsvResult.of(
                exportedProperties,
                tookMillis
            );
        } catch (RuntimeException e) {
            log.warn("CSV export failed", e);
            throw e;
        }
    }

    public static Path exportLocation(Configuration neo4jConfig, GraphStoreToFileExporterConfig config) {
        var rootPath = neo4jConfig.get(GraphStoreExportSettings.export_location_setting);
        var exportPath = rootPath != null ? rootPath.resolve(EXPORT_DIR) : null;
        return exportPath(exportPath, config);
    }

    public static Path exportPath(@Nullable Path rootPath, GraphStoreToFileExporterConfig config) {
        if (rootPath == null) {
            throw new RuntimeException(formatWithLocale(
                "The configuration option '%s' must be set.",
                GraphStoreExportSettings.export_location_setting.name()
            ));
        }

        DIRECTORY_IS_WRITABLE.validate(rootPath);

        var resolvedExportPath = rootPath.resolve(config.exportName()).normalize();
        var resolvedParent = resolvedExportPath.getParent();

        if (resolvedParent == null || !resolvedParent.startsWith(rootPath)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Illegal parameter value for parameter exportName '%s'. It attempts to write into a forbidden directory.",
                config.exportName()
            ));
        }

        if (Files.exists(resolvedExportPath)) {
            throw new IllegalArgumentException(formatWithLocale("The specified export directory '%s' already exists.", resolvedExportPath));
        }

        try {
            Files.createDirectories(resolvedExportPath);
        } catch (IOException e) {
            throw new RuntimeException("Could not create import directory.", e);
        }

        return resolvedExportPath;
    }

    @ValueClass
    public interface ExportToCsvResult {
        GraphStoreExporter.ExportedProperties importedProperties();

        long tookMillis();
    }

    private GraphStoreExporterUtil() {}
}
