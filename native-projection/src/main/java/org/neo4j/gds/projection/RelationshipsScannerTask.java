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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.PropertyReader;
import org.neo4j.gds.core.loading.RecordScannerTask;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.core.utils.RawValues;
import org.neo4j.gds.core.utils.StatementAction;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

final class RelationshipsScannerTask extends StatementAction implements RecordScannerTask {

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

                    PropertyReader<Reference> propertyReader = importer.loadProperties()
                            ? storeBackedPropertyReader(transaction)
                            : emptyPropertyReader();

                        return importer.threadLocalImporter(
                            buffer.relationshipsBatchBuffer(),
                            propertyReader
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
                for (var importer : importers) {
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

    @NotNull
    private static PropertyReader<Reference> emptyPropertyReader() {
        return (producer, propertyKeyIds, defaultValues, aggregations, atLeastOnePropertyToLoad) -> new long[propertyKeyIds.length][0];
    }

    @NotNull
    private static PropertyReader<Reference> storeBackedPropertyReader(KernelTransaction kernelTransaction) {
        return (producer, propertyKeyIds, defaultPropertyValues, aggregations, atLeastOnePropertyToLoad) -> {
            long[][] properties = new long[propertyKeyIds.length][producer.numberOfElements()];
            if (atLeastOnePropertyToLoad) {
                var read = kernelTransaction.dataRead();
                try (
                    PropertyCursor pc = kernelTransaction
                        .cursors()
                        .allocatePropertyCursor(kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())
                ) {
                    double[] relProps = new double[propertyKeyIds.length];
                    producer.forEach((index, source, target, relationshipReference, propertyReference) -> {
                        read.relationshipProperties(
                            relationshipReference,
                            source,
                            propertyReference,
                            PropertySelection.ALL_PROPERTIES,
                            pc
                        );
                        NativeRelationshipPropertyReadHelper.readProperties(
                            pc,
                            propertyKeyIds,
                            defaultPropertyValues,
                            aggregations,
                            relProps
                        );
                        for (int j = 0; j < relProps.length; j++) {
                            properties[j][index] = Double.doubleToLongBits(relProps[j]);
                        }
                    });
                }
            } else {
                producer.forEach((index, source, target, relationshipReference, propertyReference) -> {
                    for (int j = 0; j < defaultPropertyValues.length; j++) {
                        double value = aggregations[j].normalizePropertyValue(defaultPropertyValues[j]);
                        properties[j][index] = Double.doubleToLongBits(value);
                    }
                });
            }
            return properties;
        };
    }

}
