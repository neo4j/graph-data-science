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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.file.GraphStoreExporterUtil;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterConfig;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterParameters;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.core.io.file.GraphStoreExporterUtil.EXPORT_DIR;
import static org.neo4j.gds.core.io.file.GraphStoreExporterUtil.exportPath;

class ExportToCsvApplication {
    private final Log log;

    private final GraphDatabaseService graphDatabaseService;
    private final Transaction procedureTransaction;

    private final Supplier<Path> exportLocation;
    private final TaskRegistryFactory taskRegistryFactory;

    ExportToCsvApplication(
        Log log,
        GraphDatabaseService graphDatabaseService,
        Transaction procedureTransaction,
        Supplier<Path> exportLocation,
        TaskRegistryFactory taskRegistryFactory
    ) {
        this.log = log;
        this.graphDatabaseService = graphDatabaseService;
        this.procedureTransaction = procedureTransaction;
        this.exportLocation = exportLocation;
        this.taskRegistryFactory = taskRegistryFactory;
    }

    FileExportResult run(GraphName graphName, GraphStoreToFileExporterConfig configuration, GraphStore graphStore) {
        var exportParameters = new GraphStoreToFileExporterParameters(
            configuration.exportName(),
            configuration.username(),
            RelationshipType.of(configuration.defaultRelationshipType()),
            configuration.typedWriteConcurrency(),
            configuration.batchSize()
        );

        var exportLocation = this.exportLocation.get();
        var exportDirectory = readyExportDirectory(exportParameters.exportName(), exportLocation);

        var result = GraphStoreExporterUtil.export(
            graphStore,
            exportDirectory,
            exportParameters,
            neoNodeProperties(configuration.additionalNodeProperties(), graphStore),
            taskRegistryFactory,
            log,
            DefaultPool.INSTANCE
        );

        return new FileExportResult(
            graphName.getValue(),
            configuration.exportName(),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            graphStore.relationshipTypes().size(),
            result.importedProperties().nodePropertyCount(),
            result.importedProperties().relationshipPropertyCount(),
            result.tookMillis()
        );
    }

    private Optional<NeoNodeProperties> neoNodeProperties(
        PropertyMappings additionalNodeProperties,
        GraphStore graphStore
    ) {
        return NeoNodeProperties.of(
            graphStore,
            DatabaseTransactionContext.of(graphDatabaseService, procedureTransaction),
            additionalNodeProperties,
            log
        );
    }

    private Path readyExportDirectory(String exportName, Path rootPath) {
        var exportDirectory = rootPath != null ? rootPath.resolve(EXPORT_DIR) : null;

        return exportPath(exportDirectory, exportName);
    }
}
