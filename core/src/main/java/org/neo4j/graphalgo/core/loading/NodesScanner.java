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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.LongSet;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Collection;
import java.util.Collections;

public final class NodesScanner extends StatementAction implements RecordScanner {

    public static InternalImporter.CreateScanner of(
        SecureTransaction tx,
        StoreScanner<NodeReference> scanner,
        LongSet labels,
        ProgressLogger progressLogger,
        NodeImporter importer,
        @Nullable NativeNodePropertyImporter nodePropertyImporter,
        TerminationFlag terminationFlag
    ) {
        return new NodesScanner.Creator(
                tx,
                scanner,
                labels,
                progressLogger,
                importer,
                nodePropertyImporter,
                terminationFlag);
    }

    static final class Creator implements InternalImporter.CreateScanner {
        private final SecureTransaction tx;
        private final StoreScanner<NodeReference> scanner;
        private final LongSet labels;
        private final ProgressLogger progressLogger;
        private final NodeImporter importer;
        private final NativeNodePropertyImporter nodePropertyImporter;
        private final TerminationFlag terminationFlag;

        Creator(
            SecureTransaction tx,
            StoreScanner<NodeReference> scanner,
            LongSet labels,
            ProgressLogger progressLogger,
            NodeImporter importer,
            @Nullable NativeNodePropertyImporter nodePropertyImporter,
            TerminationFlag terminationFlag
        ) {
            this.tx = tx;
            this.scanner = scanner;
            this.labels = labels;
            this.progressLogger = progressLogger;
            this.importer = importer;
            this.nodePropertyImporter = nodePropertyImporter;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScanner create(int index) {
            return new NodesScanner(
                    tx,
                    terminationFlag,
                    scanner,
                    labels,
                    index,
                    progressLogger,
                    importer,
                    nodePropertyImporter
            );
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }

    }

    private final TerminationFlag terminationFlag;
    private final StoreScanner<NodeReference> scanner;
    private final LongSet labels;
    private final int scannerIndex;
    private final ProgressLogger progressLogger;
    private final NodeImporter importer;
    private final NativeNodePropertyImporter nodePropertyImporter;
    private long propertiesImported;
    private long nodesImported;

    private NodesScanner(
        SecureTransaction tx,
        TerminationFlag terminationFlag,
        StoreScanner<NodeReference> scanner,
        LongSet labels,
        int threadIndex,
        ProgressLogger progressLogger,
        NodeImporter importer,
        @Nullable NativeNodePropertyImporter nodePropertyImporter
    ) {
        super(tx);
        this.terminationFlag = terminationFlag;
        this.scanner = scanner;
        this.labels = labels;
        this.scannerIndex = threadIndex;
        this.progressLogger = progressLogger;
        this.importer = importer;
        this.nodePropertyImporter = nodePropertyImporter;
    }

    @Override
    public String threadName() {
        return "node-store-scan-" + scannerIndex;
    }

    @Override
    public void accept(KernelTransaction transaction) {
        try (StoreScanner.ScanCursor<NodeReference> cursor = scanner.getCursor(transaction)) {
            NodesBatchBuffer batches = new NodesBatchBufferBuilder()
                .nodeLabelIds(labels)
                .capacity(cursor.bufferSize())
                .hasLabelInformation(!labels.isEmpty())
                .readProperty(nodePropertyImporter != null)
                .build();
            while (batches.scan(cursor)) {
                terminationFlag.assertRunning();
                long imported = importer.importNodes(
                    batches,
                    transaction,
                    nodePropertyImporter
                );
                int batchImportedNodes = RawValues.getHead(imported);
                int batchImportedProperties = RawValues.getTail(imported);
                progressLogger.logProgress(batchImportedNodes);
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

}
