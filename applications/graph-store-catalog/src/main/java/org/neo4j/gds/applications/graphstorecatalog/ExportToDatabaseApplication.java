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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.io.GraphStoreExporter;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.io.db.GraphStoreToDatabaseExporter;
import org.neo4j.gds.core.io.db.GraphStoreToDatabaseExporterConfig;
import org.neo4j.gds.core.io.db.GraphStoreToDatabaseExporterParameters;
import org.neo4j.gds.core.io.db.ProgressTrackerExecutionMonitor;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.Optional;

class ExportToDatabaseApplication {
    private final Log log;

    private final GraphDatabaseService graphDatabaseService;
    private final Transaction procedureTransaction;

    private final TaskRegistryFactory taskRegistryFactory;
    private final UserLogRegistryFactory userLogRegistryFactory;

    ExportToDatabaseApplication(
        Log log,
        GraphDatabaseService graphDatabaseService,
        Transaction procedureTransaction,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        this.log = log;
        this.graphDatabaseService = graphDatabaseService;
        this.procedureTransaction = procedureTransaction;
        this.taskRegistryFactory = taskRegistryFactory;
        this.userLogRegistryFactory = userLogRegistryFactory;
    }

    DatabaseExportResult run(
        GraphName graphName,
        GraphStoreToDatabaseExporterConfig configuration,
        GraphStore graphStore
    ) {
        var progressTracker = new TaskProgressTracker(
            ProgressTrackerExecutionMonitor.progressTask(graphStore),
            log,
            configuration.typedWriteConcurrency(),
            configuration.jobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        @SuppressWarnings("removal") var parameters = new GraphStoreToDatabaseExporterParameters(
            configuration.databaseName(),
            new Concurrency(configuration.writeConcurrency()),
            configuration.batchSize(),
            RelationshipType.of(configuration.defaultRelationshipType()),
            configuration.databaseFormat(),
            configuration.enableDebugLog()
        );

        var exporter = GraphStoreToDatabaseExporter.of(
            graphStore,
            graphDatabaseService,
            parameters,
            neoNodeProperties(configuration.additionalNodeProperties(), graphStore),
            log,
            progressTracker
        );

        var start = System.nanoTime();
        var exportedProperties = runAndHandleProgressTracker(progressTracker, exporter);
        var end = System.nanoTime();

        return new DatabaseExportResult(
            graphName.getValue(),
            configuration.databaseName(),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            graphStore.relationshipTypes().size(),
            exportedProperties.nodePropertyCount(),
            exportedProperties.relationshipPropertyCount(),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)
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

    private GraphStoreExporter.ExportedProperties runAndHandleProgressTracker(
        ProgressTracker progressTracker,
        GraphStoreToDatabaseExporter exporter
    ) {
        try {
            return exporter.run();
        } catch (RuntimeException e) {
            // the progress tracker is instantiated in this class
            // so, we need to make sure it closes as well
            progressTracker.endSubTaskWithFailure();
            throw e;
        }
    }
}
