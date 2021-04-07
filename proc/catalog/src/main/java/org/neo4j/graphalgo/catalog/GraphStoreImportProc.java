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
package org.neo4j.graphalgo.catalog;

import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.export.file.CsvToGraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.config.ConcurrencyConfig.CONCURRENCY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateConfig.READ_CONCURRENCY_KEY;
import static org.neo4j.procedure.Mode.READ;

public class GraphStoreImportProc extends BaseProc {

    @Internal
    @Procedure(name = "gds.graphs.load", mode = READ)
    @Description("Loads persisted graph stores from disk.")
    public Stream<FileImportResult> persist(
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherMapWrapper = CypherMapWrapper.create(configuration);
        var readConcurrency = cypherMapWrapper.getInt(READ_CONCURRENCY_KEY, cypherMapWrapper.getInt(CONCURRENCY_KEY, 4));
        var config = ImmutableGraphStoreToFileExporterConfig.builder()
            .writeConcurrency(readConcurrency)
            .exportName("")
            .includeMetaData(true)
            .build();

        return runWithExceptionLogging(
            "Graph store loading failed",
            () -> {
                try {
                    return getImportPaths().stream().map(path -> {
                        var graphStoreImporter = CsvToGraphStoreExporter.create(config, path);

                        var start = System.nanoTime();
                        graphStoreImporter.run(allocationTracker());
                        var end = System.nanoTime();

                        var graphStore = graphStoreImporter.graphStore();

                        var graphName = path.getFileName().toString();
                        var createConfig = GraphCreateFromStoreConfig.emptyWithName(
                            username(),
                            graphName
                        );
                        GraphStoreCatalog.set(createConfig, graphStore);

                        return new FileImportResult(
                            graphName,
                            graphStore.nodeCount(),
                            graphStore.relationshipCount(),
                            graphStore.relationshipTypes().size(),
                            graphStore.schema().nodeSchema().allProperties().size(),
                            graphStore.schema().relationshipSchema().allProperties().size(),
                            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)
                        );
                    });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        );
    }

    private List<Path> getImportPaths() throws IOException {
        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);
        var exportLocation = neo4jConfig.get(GraphStoreExportSettings.export_location_setting);

        return Files.list(exportLocation)
            .peek(CsvToGraphStoreExporter.DIRECTORY_IS_READABLE::validate)
            .collect(Collectors.toList());
    }

    public static class FileImportResult extends GraphStoreExportProc.GraphStoreExportResult {

        public FileImportResult(
            String graphName,
            long nodeCount,
            long relationshipCount,
            long relationshipTypeCount,
            long nodePropertyCount,
            long relationshipPropertyCount,
            long writeMillis
        ) {
            super(
                graphName,
                nodeCount,
                relationshipCount,
                relationshipTypeCount,
                nodePropertyCount,
                relationshipPropertyCount,
                writeMillis
            );
        }
    }
}
