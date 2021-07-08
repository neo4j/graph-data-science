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
package org.neo4j.gds;

import org.apache.commons.io.file.PathUtils;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.io.file.CsvGraphStoreImporter;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableCsvGraphStoreImporterConfig;
import org.neo4j.graphalgo.core.utils.io.file.csv.AutoloadFlagVisitor;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public final class AuraGraphRestorer {

    public static void restore(Path importDir, Log log) throws IOException {
        var config = ImmutableCsvGraphStoreImporterConfig.builder().build();
        getImportPaths(importDir).forEach(path -> {
            var graphStoreImporter = CsvGraphStoreImporter.create(config, path, log);

            graphStoreImporter.run(AllocationTracker.empty());

            var graphStore = graphStoreImporter.userGraphStore();

            var graphName = path.getFileName().toString();
            var createConfig = GraphCreateFromStoreConfig.emptyWithName(
                graphStore.userName(),
                graphName
            );
            GraphStoreCatalog.set(createConfig, graphStore.graphStore());
            try {
                PathUtils.deleteDirectory(path);
            } catch (IOException e) {
                log.warn("Failed to remove imported graph '%s'", graphName);
                throw new UncheckedIOException(e);
            }
        });
    }

    private static Stream<Path> getImportPaths(Path importDir) throws IOException {
        return Files.list(importDir)
            .peek(CsvGraphStoreImporter.DIRECTORY_IS_READABLE::validate)
            .filter(graphDir -> Files.exists(graphDir.resolve(AutoloadFlagVisitor.AUTOLOAD_FILE_NAME)));
    }


    private AuraGraphRestorer() {}
}
