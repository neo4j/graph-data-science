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
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.io.GraphStoreExporterBaseConfig;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.db.GraphStoreToDatabaseExporter;
import org.neo4j.gds.core.io.db.GraphStoreToDatabaseExporterConfig;
import org.neo4j.gds.core.io.db.GraphStoreToDatabaseExporterParameters;
import org.neo4j.gds.core.io.db.ProgressTrackerExecutionMonitor;
import org.neo4j.gds.core.io.file.GraphStoreExporterUtil;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterConfig;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterParameters;
import org.neo4j.gds.core.io.file.csv.estimation.CsvExportEstimation;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.preconditions.ClusterRestrictions;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.core.io.file.GraphStoreExporterUtil.exportLocation;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class GraphStoreExportProc extends BaseProc {

    private static final String DESCRIPTION = "Exports a named graph to CSV files.";
    private static final String DESCRIPTION_ESTIMATE = "Estimate the required disk space for exporting a named graph to CSV files.";

    @Procedure(name = "gds.graph.export", mode = READ)
    @Description("Exports a named graph into a new offline Neo4j database.")
    public Stream<DatabaseExportResult> database(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ClusterRestrictions.disallowRunningOnCluster(databaseService, "Export a graph to Neo4j database");

        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToDatabaseExporterConfig.of(cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        var result = runWithExceptionLogging(
            "Graph creation failed",
            () -> {
                var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();

                validateGraphStore(graphStore, exportConfig);

                var progressTracker = new TaskProgressTracker(
                    ProgressTrackerExecutionMonitor.progressTask(graphStore),
                    executionContext().log(),
                    exportConfig.typedWriteConcurrency(),
                    exportConfig.jobId(),
                    executionContext().taskRegistryFactory(),
                    executionContext().userLogRegistryFactory()
                );

                var parameters = new GraphStoreToDatabaseExporterParameters(
                    exportConfig.databaseName(),
                    new Concurrency(exportConfig.writeConcurrency()),
                    exportConfig.batchSize(),
                    RelationshipType.of(exportConfig.defaultRelationshipType()),
                    exportConfig.databaseFormat(),
                    exportConfig.enableDebugLog()
                );

                var exporter = GraphStoreToDatabaseExporter.of(
                    graphStore,
                    databaseService,
                    parameters,
                    neoNodeProperties(exportConfig.additionalNodeProperties(), graphStore),
                    executionContext().log(),
                    progressTracker
                );

                try {
                    var start = System.nanoTime();
                    var exportedProperties = exporter.run();
                    var end = System.nanoTime();

                    return new DatabaseExportResult(
                        graphName,
                        exportConfig.databaseName(),
                        graphStore.nodeCount(),
                        graphStore.relationshipCount(),
                        graphStore.relationshipTypes().size(),
                        exportedProperties.nodePropertyCount(),
                        exportedProperties.relationshipPropertyCount(),
                        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)
                    );
                } catch (Exception e) {
                    // Ideally we should not have this logic on the proc level
                    // the progress tracker is instantiated in this proc
                    // so we need to make sure it closes as well
                    progressTracker.endSubTaskWithFailure();
                    throw e;
                }
            }
        );

        return Stream.of(result);
    }

    @Internal
    @Procedure(name = "gds.beta.graph.export.csv", mode = READ, deprecatedBy = "gds.graph.export.csv")
    @Description(DESCRIPTION)
    @Deprecated(forRemoval = true)
    public Stream<FileExportResult> csvDeprecated(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.graph.export.csv");

        executionContext()
            .log()
            .warn("Procedure `gds.beta.graph.export.csv` has been deprecated, please use `gds.graph.export.csv`.");

        return csv(graphName, configuration);
    }

    @Procedure(name = "gds.graph.export.csv", mode = READ)
    @Description(DESCRIPTION)
    public Stream<FileExportResult> csv(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToFileExporterConfig.of(username(), cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();
        validateGraphStore(graphStore, exportConfig);

        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(databaseService, Config.class);

        var exportParameters = new GraphStoreToFileExporterParameters(
            exportConfig.exportName(),
            exportConfig.username(),
            true,
            RelationshipType.of(exportConfig.defaultRelationshipType()),
            exportConfig.typedWriteConcurrency(),
            exportConfig.batchSize()
        );
        var result = GraphStoreExporterUtil.export(
            graphStore,
            exportLocation(neo4jConfig, exportParameters.exportName()),
            exportParameters,
            neoNodeProperties(exportConfig.additionalNodeProperties(), graphStore),
            executionContext().taskRegistryFactory(),
            executionContext().log(),
            DefaultPool.INSTANCE
        );

        return Stream.of(
            new FileExportResult(
                graphName,
                exportConfig.exportName(),
                graphStore.nodeCount(),
                graphStore.relationshipCount(),
                graphStore.relationshipTypes().size(),
                result.importedProperties().nodePropertyCount(),
                result.importedProperties().relationshipPropertyCount(),
                result.tookMillis()
            )
        );
    }

    @Internal
    @Procedure(name = "gds.beta.graph.export.csv.estimate", mode = READ, deprecatedBy = "gds.graph.export.csv.estimate")
    @Description(DESCRIPTION_ESTIMATE)
    @Deprecated(forRemoval = true)
    public Stream<MemoryEstimateResult> csvEstimateDeprecated(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.graph.export.csv.estimate");
        executionContext()
            .log()
            .warn(
                "Procedure `gds.beta.graph.export.csv.estimate` has been deprecated, please use `gds.graph.export.csv.estimate`."
            );

        return csvEstimate(graphName, configuration);
    }

    @Procedure(name = "gds.graph.export.csv.estimate", mode = READ)
    @Description(DESCRIPTION_ESTIMATE)
    public Stream<MemoryEstimateResult> csvEstimate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToCsvEstimationConfig.of(username(), cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        var estimate = runWithExceptionLogging(
            "CSV export estimation failed",
            () -> {
                var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();


                var dimensions = GraphDimensions.of(graphStore.nodeCount(), graphStore.relationshipCount());
                var memoryTree = CsvExportEstimation
                    .estimate(graphStore, exportConfig.samplingFactor())
                    .estimate(dimensions, new Concurrency(1));
                return new MemoryTreeWithDimensions(memoryTree, dimensions);
            }
        );

        return Stream.of(new MemoryEstimateResult(estimate));
    }

    private Optional<NeoNodeProperties> neoNodeProperties(
        PropertyMappings additionalNodeProperties,
        GraphStore graphStore
    ) {
        return NeoNodeProperties.of(
            graphStore,
            DatabaseTransactionContext.of(databaseService, procedureTransaction),
            additionalNodeProperties,
            executionContext().log()
        );
    }

    private void validateGraphStore(GraphStore graphStore, GraphStoreExporterBaseConfig exportConfig) {
        validateReadAccess(graphStore, !exportConfig.additionalNodeProperties().mappings().isEmpty());
        validateAdditionalNodeProperties(graphStore, exportConfig.additionalNodeProperties());
    }

    private void validateReadAccess(GraphStore graphStore, boolean exportAdditionalNodeProperties) {
        if (exportAdditionalNodeProperties && !graphStore.capabilities().canWriteToLocalDatabase()) {
            throw new IllegalArgumentException("Exporting additional node properties is not allowed for this graph.");
        }
    }

    private void validateAdditionalNodeProperties(GraphStore graphStore, PropertyMappings additionalNodeProperties) {
        var nodeProperties = graphStore.nodePropertyKeys();
        var duplicateProperties = additionalNodeProperties
            .stream()
            .map(PropertyMapping::neoPropertyKey)
            .filter(nodeProperties::contains)
            .collect(Collectors.toList());
        if (!duplicateProperties.isEmpty()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "The following provided additional node properties are already present in the in-memory graph: %s",
                    StringJoining.joinVerbose(duplicateProperties)
                )
            );
        }
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
