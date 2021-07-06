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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.core.utils.export.GraphStoreExporter;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.neo4j.graphalgo.core.utils.export.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class GraphStoreExporterUtil {

    public static ExportToCsvResult runGraphStoreExportToCsv(
        GraphStore graphStore,
        Config neo4jConfig,
        GraphStoreToFileExporterConfig exportConfig,
        Log log,
        AllocationTracker allocationTracker
    ) {
        try {
            var exportPath = getExportPath(neo4jConfig, exportConfig);
            var exporter = GraphStoreToFileExporter.csv(graphStore, exportConfig, exportPath);

            var start = System.nanoTime();
            var importedProperties = exporter.run(allocationTracker);
            var end = System.nanoTime();

            var tookMillis = TimeUnit.NANOSECONDS.toMillis(end - start);
            log.info("Export completed in %s ms", tookMillis);
            return ImmutableExportToCsvResult.of(
                importedProperties,
                tookMillis
            );
        } catch (RuntimeException e) {
            log.warn("CSV export failed", e);
            throw e;
        }
    }

    public static Path getExportPath(Config neo4jConfig, GraphStoreToFileExporterConfig config) {
        var exportLocation = neo4jConfig.get(GraphStoreExportSettings.export_location_setting);

        if (exportLocation == null) {
            throw new RuntimeException(formatWithLocale(
                "The configuration option '%s' must be set.",
                GraphStoreExportSettings.export_location_setting.name()
            ));
        }

        DIRECTORY_IS_WRITABLE.validate(exportLocation);

        var resolvedExportPath = exportLocation.resolve(config.exportName()).normalize();

        if (!resolvedExportPath.startsWith(exportLocation)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Illegal parameter value for parameter exportName=%s. It attempts to write into forbidden directory %s.",
                config.exportName(),
                resolvedExportPath
            ));
        }

        if (resolvedExportPath.toFile().exists()) {
            throw new IllegalArgumentException("The specified import directory already exists.");
        }

        try {
            Files.createDirectories(resolvedExportPath);
        } catch (IOException e) {
            throw new RuntimeException("Could not create import directory", e);
        }

        return resolvedExportPath;
    }

    @ValueClass
    public interface ExportToCsvResult {
        GraphStoreExporter.ImportedProperties importedProperties();

        long tookMillis();
    }

    private GraphStoreExporterUtil() {}
}
