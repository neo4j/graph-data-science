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
package org.neo4j.gds.catalog;

import org.neo4j.configuration.Config;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.utils.io.GraphStoreExporterBaseConfig;
import org.neo4j.graphalgo.core.utils.io.NeoNodeProperties;
import org.neo4j.graphalgo.core.utils.io.db.GraphStoreToDatabaseExporter;
import org.neo4j.graphalgo.core.utils.io.db.GraphStoreToDatabaseExporterConfig;
import org.neo4j.graphalgo.core.utils.io.file.GraphStoreExporterUtil;
import org.neo4j.graphalgo.core.utils.io.file.GraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.io.file.csv.estimation.CsvExportEstimation;
import org.neo4j.graphalgo.core.utils.io.file.csv.estimation.GraphStoreToCsvEstimationConfig;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.io.file.GraphStoreExporterUtil.exportLocation;
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
                var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();

                var exporter = GraphStoreToDatabaseExporter.of(
                    graphStore,
                    api,
                    exportConfig,
                    neoNodeProperties(exportConfig, graphStore)
                );

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

        var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();
        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);

        var result = GraphStoreExporterUtil.export(
            graphStore,
            exportLocation(neo4jConfig, exportConfig),
            exportConfig,
            neoNodeProperties(exportConfig, graphStore),
            log,
            allocationTracker()
        );

        return Stream.of(new FileExportResult(
            graphName,
            exportConfig.exportName(),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            graphStore.relationshipTypes().size(),
            result.importedProperties().nodePropertyCount(),
            result.importedProperties().relationshipPropertyCount(),
            result.tookMillis()
        ));
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
                var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();


                var dimensions = GraphDimensions.of(graphStore.nodeCount(), graphStore.relationshipCount());
                var memoryTree = CsvExportEstimation
                    .estimate(graphStore, exportConfig.samplingFactor())
                    .estimate(dimensions, 1);
                return new MemoryTreeWithDimensions(memoryTree, dimensions);
            }
        );

        return Stream.of(new MemoryEstimateResult(estimate));
    }

    private Optional<NeoNodeProperties> neoNodeProperties(
        GraphStoreExporterBaseConfig exportConfig,
        GraphStore graphStore
    ) {
        return NeoNodeProperties.of(
            graphStore,
            TransactionContext.of(api, procedureTransaction),
            exportConfig.additionalNodeProperties(),
            log
        );
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
