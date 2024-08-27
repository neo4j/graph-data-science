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
package org.neo4j.gds.compat._522;

import org.neo4j.configuration.Config;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.gds.compat.batchimport.BatchImporter;
import org.neo4j.gds.compat.batchimport.ExecutionMonitor;
import org.neo4j.gds.compat.batchimport.ImportConfig;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.compat.batchimport.input.Collector;
import org.neo4j.gds.compat.batchimport.input.Estimates;
import org.neo4j.gds.compat.batchimport.input.ReadableGroups;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

import java.io.OutputStream;
import java.util.function.LongConsumer;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    @Override
    public BatchImporter instantiateBlockBatchImporter(
        DatabaseLayout dbLayout,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
        Monitor monitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return BatchImporterCompat.instantiateBlockBatchImporter(
            dbLayout,
            fileSystem,
            config,
            monitor,
            logService,
            dbConfig,
            jobScheduler,
            badCollector
        );
    }

    @Override
    public BatchImporter instantiateRecordBatchImporter(
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        ImportConfig config,
        ExecutionMonitor executionMonitor,
        LogService logService,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        return BatchImporterCompat.instantiateRecordBatchImporter(
            directoryStructure,
            fileSystem,
            config,
            executionMonitor,
            logService,
            dbConfig,
            jobScheduler,
            badCollector
        );
    }

    @Override
    public ExecutionMonitor newCoarseBoundedProgressExecutionMonitor(
        long highNodeId,
        long highRelationshipId,
        int batchSize,
        LongConsumer progress,
        LongConsumer outNumberOfBatches
    ) {
        return BatchImporterCompat.newCoarseBoundedProgressExecutionMonitor(
            highNodeId,
            highRelationshipId,
            batchSize,
            progress,
            outNumberOfBatches
        );
    }

    @Override
    public ReadableGroups newGroups() {
        return BatchImporterCompat.newGroups();
    }

    @Override
    public ReadableGroups newInitializedGroups() {
        return BatchImporterCompat.newInitializedGroups();
    }

    @Override
    public Collector emptyCollector() {
        return BatchImporterCompat.emptyCollector();
    }

    @Override
    public Collector badCollector(OutputStream outputStream, int batchSize) {
        return BatchImporterCompat.badCollector(outputStream, batchSize);
    }

    @Override
    public Estimates knownEstimates(
        long numberOfNodes,
        long numberOfRelationships,
        long numberOfNodeProperties,
        long numberOfRelationshipProperties,
        long sizeOfNodeProperties,
        long sizeOfRelationshipProperties,
        long numberOfNodeLabels
    ) {
        return BatchImporterCompat.knownEstimates(
            numberOfNodes,
            numberOfRelationships,
            numberOfNodeProperties,
            numberOfRelationshipProperties,
            sizeOfNodeProperties,
            sizeOfRelationshipProperties,
            numberOfNodeLabels
        );
    }

    @Override
    public void relationshipProperties(
        Read read,
        long relationshipReference,
        long startNodeReference,
        Reference reference,
        PropertySelection selection,
        PropertyCursor cursor
    ) {
        read.relationshipProperties(relationshipReference, startNodeReference, reference, selection, cursor);
    }
}
