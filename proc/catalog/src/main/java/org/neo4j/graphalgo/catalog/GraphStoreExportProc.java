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
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.export.db.GraphStoreToDatabaseExporter;
import org.neo4j.graphalgo.core.utils.export.db.GraphStoreToDatabaseExporterConfig;
import org.neo4j.graphalgo.core.utils.export.file.GraphStoreToFileExporter;
import org.neo4j.graphalgo.core.utils.export.file.GraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.export.file.csv.estimation.CsvExportEstimation;
import org.neo4j.graphalgo.core.utils.export.file.csv.estimation.GraphStoreToCsvEstimationConfig;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.export.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class GraphStoreExportProc extends BaseProc {

    @Procedure(name = "gds.graph.export", mode = READ)
    @Description("Exports a named graph into a new offline Neo4j database.")
    public Stream<DatabaseExportResult> database(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToDatabaseExporterConfig.of(username(), cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        var result = runWithExceptionLogging(
            "Graph creation failed", () -> {
                var graphStore = GraphStoreCatalog.get(username(), databaseId(), graphName).graphStore();

                var exporter = GraphStoreToDatabaseExporter.newExporter(graphStore, api, exportConfig);

                var start = System.nanoTime();
                var importedProperties = exporter.run(allocationTracker());
                var end = System.nanoTime();

                return new DatabaseExportResult(
                    graphName,
                    exportConfig.dbName(),
                    graphStore.nodeCount(),
                    graphStore.relationshipCount(),
                    graphStore.relationshipTypes().size(),
                    importedProperties.nodePropertyCount(),
                    importedProperties.relationshipPropertyCount(),
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)
                );
            }
        );

        return Stream.of(result);
    }

    @Procedure(name = "gds.beta.graph.export.csv", mode = READ)
    @Description("Exports a named graph to CSV files.")
    public Stream<FileExportResult> csv(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToFileExporterConfig.of(username(), cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        return Stream.of(runGraphStoreExportToCsv(graphName, exportConfig));
    }

    @Internal
    @Procedure(name = "gds.graphs.persist", mode = READ)
    @Description("Persists a graph store to disk.")
    public Stream<FileExportResult> persist(
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);

        return runWithExceptionLogging(
            "GraphStore persistance failed",
            () -> GraphStoreCatalog.getGraphStores(username(), databaseId()).keySet().stream().map(createConfig -> {
                var exportConfig = cypherConfig
                    .withBoolean("includeMetaData", true)
                    .withString("exportName", createConfig.graphName());

                var config = GraphStoreToFileExporterConfig.of(username(), exportConfig);

                return runGraphStoreExportToCsv(createConfig.graphName(), config);
            })
        );
    }

    private FileExportResult runGraphStoreExportToCsv(String graphName, GraphStoreToFileExporterConfig exportConfig) {
        return runWithExceptionLogging(
            "CSV export failed", () -> {
                var exportPath = getExportPath(exportConfig);

                var graphStore = GraphStoreCatalog.get(username(), databaseId(), graphName).graphStore();

                var exporter = GraphStoreToFileExporter.csv(graphStore, exportConfig, exportPath);

                var start = System.nanoTime();
                var importedProperties = exporter.run(allocationTracker());
                var end = System.nanoTime();

                return new FileExportResult(
                    graphName,
                    exportConfig.exportName(),
                    graphStore.nodeCount(),
                    graphStore.relationshipCount(),
                    graphStore.relationshipTypes().size(),
                    importedProperties.nodePropertyCount(),
                    importedProperties.relationshipPropertyCount(),
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)
                );
            }
        );
    }

    @Procedure(name = "gds.beta.graph.export.csv.estimate", mode = READ)
    @Description("Estimate the required disk space for exporting a named graph to CSV files.")
    public Stream<MemoryEstimateResult> csvEstimate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToCsvEstimationConfig.of(username(), cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        var estimate = runWithExceptionLogging(
            "CSV export estimation failed", () -> {
                var graphStore = GraphStoreCatalog.get(username(), databaseId(), graphName).graphStore();


                var dimensions = GraphDimensions.of(graphStore.nodeCount(), graphStore.relationshipCount());
                var memoryTree = CsvExportEstimation
                    .estimate(graphStore, exportConfig.samplingFactor())
                    .estimate(dimensions, 1);
                return new MemoryTreeWithDimensions(memoryTree, dimensions);
            }
        );

        return Stream.of(new MemoryEstimateResult(estimate));
    }

    private Path getExportPath(GraphStoreToFileExporterConfig config) {
        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);
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

    @SuppressWarnings("unused")
    public abstract static class GraphStoreExportResult {
        public final String graphName;
        public final long nodeCount;
        public final long relationshipCount;
        public final long relationshipTypeCount;
        public final long nodePropertyCount;
        public final long relationshipPropertyCount;
        public final long writeMillis;

        public GraphStoreExportResult(
            String graphName,
            long nodeCount,
            long relationshipCount,
            long relationshipTypeCount,
            long nodePropertyCount,
            long relationshipPropertyCount,
            long writeMillis
        ) {
            this.graphName = graphName;
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.relationshipTypeCount = relationshipTypeCount;
            this.nodePropertyCount = nodePropertyCount;
            this.relationshipPropertyCount = relationshipPropertyCount;
            this.writeMillis = writeMillis;
        }
    }

    @SuppressWarnings("unused")
    public static class DatabaseExportResult extends GraphStoreExportResult {
        public final String dbName;

        public DatabaseExportResult(
            String graphName,
            String dbName,
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
            this.dbName = dbName;
        }
    }

    @SuppressWarnings("unused")
    public static class FileExportResult extends GraphStoreExportResult {
        public final String exportName;

        public FileExportResult(
            String graphName,
            String exportName,
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
            this.exportName = exportName;
        }
    }
}
