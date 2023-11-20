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
package org.neo4j.gds.projection;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.RecordScannerTask;
import org.neo4j.gds.core.loading.RelationshipReference;
import org.neo4j.gds.core.loading.ScanState;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.core.loading.StoreScanner;
import org.neo4j.gds.core.loading.ThreadLocalSingleTypeRelationshipImporter;
import org.neo4j.gds.core.utils.RawValues;
import org.neo4j.gds.core.utils.StatementAction;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public final class RelationshipsScannerTask extends StatementAction implements RecordScannerTask {

    public static RecordScannerTaskRunner.RecordScannerTaskFactory factory(
        GraphLoaderContext loadingContext,
        ProgressTracker progressTracker,
        IdMap idMap,
        StoreScanner<RelationshipReference> scanner,
        Collection<SingleTypeRelationshipImporter> singleTypeRelationshipImporters
    ) {
        return new Factory(
            loadingContext.transactionContext(),
            progressTracker,
            idMap,
            scanner,
            singleTypeRelationshipImporters,
            loadingContext.terminationFlag()
        );
    }

    static final class Factory implements RecordScannerTaskRunner.RecordScannerTaskFactory {
        private final TransactionContext tx;
        private final ProgressTracker progressTracker;
        private final IdMap idMap;
        private final StoreScanner<RelationshipReference> scanner;
        private final Collection<SingleTypeRelationshipImporter> singleTypeRelationshipImporters;
        private final TerminationFlag terminationFlag;

        Factory(
            TransactionContext tx,
            ProgressTracker progressTracker,
            IdMap idMap,
            StoreScanner<RelationshipReference> scanner,
            Collection<SingleTypeRelationshipImporter> singleTypeRelationshipImporters,
            TerminationFlag terminationFlag
        ) {
            this.tx = tx;
            this.progressTracker = progressTracker;
            this.idMap = idMap;
            this.scanner = scanner;
            this.singleTypeRelationshipImporters = singleTypeRelationshipImporters;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScannerTask create(final int taskIndex) {
            return new RelationshipsScannerTask(
                tx,
                terminationFlag,
                progressTracker,
                idMap,
                scanner,
                taskIndex,
                singleTypeRelationshipImporters
            );
        }

        @Override
        public Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks() {
            return singleTypeRelationshipImporters.stream()
                .flatMap(factory -> factory.adjacencyListBuilderTasks(Optional.empty()).stream())
                .collect(Collectors.toList());
        }
    }

    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private final IdMap idMap;
    private final StoreScanner<RelationshipReference> scanner;
    private final int taskIndex;
    private final Collection<SingleTypeRelationshipImporter> singleTypeRelationshipImporters;

    private long relationshipsImported;
    private long weightsImported;

    private RelationshipsScannerTask(
        TransactionContext tx,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        IdMap idMap,
        StoreScanner<RelationshipReference> scanner,
        int taskIndex,
        Collection<SingleTypeRelationshipImporter> singleTypeRelationshipImporters
    ) {
        super(tx);
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.idMap = idMap;
        this.scanner = scanner;
        this.taskIndex = taskIndex;
        this.singleTypeRelationshipImporters = singleTypeRelationshipImporters;
    }

    @Override
    public String threadName() {
        return "relationship-store-scan-" + taskIndex;
    }

    @Override
    public void accept(KernelTransaction transaction) {
        try (StoreScanner.ScanCursor<RelationshipReference> cursor = scanner.createCursor(transaction)) {
            // create an importer including a dedicated batch buffer for each relationship type that we load
            var buffers = new BufferedRelationshipConsumer[this.singleTypeRelationshipImporters.size()];
            var idx = new MutableInt(0);
            var importers = this.singleTypeRelationshipImporters.stream()
                .map(importer -> {
                        var buffer = new BufferedRelationshipConsumerBuilder()
                            .idMap(idMap)
                            .type(importer.typeId())
                            .skipDanglingRelationships(importer.skipDanglingRelationships())
                            .capacity(scanner.bufferSize())
                            .build();

                        buffers[idx.getAndIncrement()] = buffer;

                        return importer.threadLocalImporter(
                            buffer.relationshipsBatchBuffer(),
                            transaction
                        );
                    }
                ).collect(Collectors.toList());

            var compositeBuffer = new BufferedCompositeRelationshipConsumerBuilder()
                .buffers(buffers)
                .build();

            long allImportedRels = 0L;
            long allImportedWeights = 0L;
            var scanState = ScanState.of();

            while (scanState.scan(cursor, compositeBuffer)) {
                terminationFlag.assertRunning();
                long imported = 0L;
                for (ThreadLocalSingleTypeRelationshipImporter importer : importers) {
                    imported += importer.importRelationships();
                }
                int importedRels = RawValues.getHead(imported);
                int importedWeights = RawValues.getTail(imported);
                progressTracker.logProgress(importedRels);
                allImportedRels += importedRels;
                allImportedWeights += importedWeights;
            }
            relationshipsImported = allImportedRels;
            weightsImported = allImportedWeights;
        }
    }

    @Override
    public long propertiesImported() {
        return weightsImported;
    }

    @Override
    public long recordsImported() {
        return relationshipsImported;
    }

}
