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

import com.carrotsearch.hppc.LongSet;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.NodeImporter;
import org.neo4j.gds.core.loading.RecordScannerTask;
import org.neo4j.gds.core.utils.RawValues;
import org.neo4j.gds.core.utils.StatementAction;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Collection;
import java.util.Collections;

public final class NodesScannerTask extends StatementAction implements RecordScannerTask {

    private final TerminationFlag terminationFlag;
    private final StoreScanner<NodeReference> scanner;
    private final long highestPossibleNodeCount;
    private final LongSet labels;
    private final int taskIndex;
    private final ProgressTracker progressTracker;
    private final NodeImporter importer;
    private final NativeNodePropertyImporter nodePropertyImporter;
    private long propertiesImported;
    private long nodesImported;

    private NodesScannerTask(
        TransactionContext tx,
        TerminationFlag terminationFlag,
        StoreScanner<NodeReference> scanner,
        long highestPossibleNodeCount,
        LongSet labels,
        int taskIndex,
        ProgressTracker progressTracker,
        NodeImporter importer,
        @Nullable NativeNodePropertyImporter nodePropertyImporter
    ) {
        super(tx);
        this.terminationFlag = terminationFlag;
        this.scanner = scanner;
        this.highestPossibleNodeCount = highestPossibleNodeCount;
        this.labels = labels;
        this.taskIndex = taskIndex;
        this.progressTracker = progressTracker;
        this.importer = importer;
        this.nodePropertyImporter = nodePropertyImporter;
    }

    @Override
    public String threadName() {
        return "node-store-scan-" + taskIndex;
    }

    @Override
    public void accept(KernelTransaction transaction) {
        try (StoreScanner.ScanCursor<NodeReference> cursor = scanner.createCursor(transaction)) {
            var nodesBatchBuffer = new BufferedNodeConsumerBuilder()
                .highestPossibleNodeCount(highestPossibleNodeCount)
                .nodeLabelIds(labels)
                .capacity(scanner.bufferSize())
                .hasLabelInformation(labels.size() > 1)
                .readProperty(nodePropertyImporter != null)
                .build();

            var scanState = ScanState.of();
            while (scanState.scan(cursor, nodesBatchBuffer)) {

                terminationFlag.assertRunning();
                long imported = importNodes(
                    nodesBatchBuffer,
                    transaction,
                    nodePropertyImporter
                );
                int batchImportedNodes = RawValues.getHead(imported);
                int batchImportedProperties = RawValues.getTail(imported);
                progressTracker.logProgress(batchImportedNodes);
                nodesImported += batchImportedNodes;
                propertiesImported += batchImportedProperties;
            }
        }
    }

    @Override
    public long propertiesImported() {
        return propertiesImported;
    }

    @Override
    public long recordsImported() {
        return nodesImported;
    }

    private long importNodes(
        BufferedNodeConsumer bufferedNodeConsumer,
        KernelTransaction kernelTransaction,
        @Nullable NativeNodePropertyImporter propertyImporter
    ) {
        return importer.importNodes(
            bufferedNodeConsumer.nodesBatchBuffer(),
            (nodeReference, labelIds, propertiesReference) -> {
                if (propertyImporter != null) {
                    return propertyImporter.importProperties(
                        nodeReference,
                        labelIds,
                        propertiesReference,
                        kernelTransaction
                    );
                } else {
                    return 0;
                }
            }
        );
    }

    public static RecordScannerTaskRunner.RecordScannerTaskFactory factory(
        TransactionContext tx,
        StoreScanner<NodeReference> scanner,
        long highestPossibleNodeCount,
        LongSet labels,
        ProgressTracker progressTracker,
        NodeImporter nodeImporter,
        @Nullable NativeNodePropertyImporter nodePropertyImporter,
        TerminationFlag terminationFlag
    ) {
        return new Factory(
            tx,
            scanner,
            highestPossibleNodeCount,
            labels,
            progressTracker,
            nodeImporter,
            nodePropertyImporter,
            terminationFlag
        );
    }

    static final class Factory implements RecordScannerTaskRunner.RecordScannerTaskFactory {
        private final TransactionContext tx;
        private final StoreScanner<NodeReference> scanner;
        private final long highestPossibleNodeCount;
        private final LongSet labels;
        private final ProgressTracker progressTracker;
        private final NodeImporter nodeImporter;
        private final NativeNodePropertyImporter nodePropertyImporter;
        private final TerminationFlag terminationFlag;

        Factory(
            TransactionContext tx,
            StoreScanner<NodeReference> scanner,
            long highestPossibleNodeCount,
            LongSet labels,
            ProgressTracker progressTracker,
            NodeImporter nodeImporter,
            @Nullable NativeNodePropertyImporter nodePropertyImporter,
            TerminationFlag terminationFlag
        ) {
            this.tx = tx;
            this.scanner = scanner;
            this.highestPossibleNodeCount = highestPossibleNodeCount;
            this.labels = labels;
            this.progressTracker = progressTracker;
            this.nodeImporter = nodeImporter;
            this.nodePropertyImporter = nodePropertyImporter;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScannerTask create(int taskIndex) {
            return new NodesScannerTask(
                tx,
                terminationFlag,
                scanner,
                highestPossibleNodeCount,
                labels,
                taskIndex,
                progressTracker,
                nodeImporter,
                nodePropertyImporter
            );
        }

        @Override
        public Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks() {
            return Collections.emptyList();
        }

    }
}
