/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.Collections;

final class NodesScanner extends StatementAction implements RecordScanner {

    static InternalImporter.CreateScanner of(
            GraphDatabaseAPI api,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            LongSet labels,
            ProgressLogger progressLogger,
            NodeImporter importer,
            TerminationFlag terminationFlag) {
        return new NodesScanner.Creator(
                api,
                scanner,
                labels,
                progressLogger,
                importer,
                terminationFlag);
    }

    static final class Creator implements InternalImporter.CreateScanner {
        private final GraphDatabaseAPI api;
        private final AbstractStorePageCacheScanner<NodeRecord> scanner;
        private final LongSet labels;
        private final ProgressLogger progressLogger;
        private final NodeImporter importer;
        private final TerminationFlag terminationFlag;

        Creator(
                GraphDatabaseAPI api,
                AbstractStorePageCacheScanner<NodeRecord> scanner,
                LongSet labels,
                ProgressLogger progressLogger,
                NodeImporter importer, 
                TerminationFlag terminationFlag) {
            this.api = api;
            this.scanner = scanner;
            this.labels = labels;
            this.progressLogger = progressLogger;
            this.importer = importer;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScanner create(int index) {
            return new NodesScanner(
                    api,
                    terminationFlag,
                    scanner,
                    labels,
                    index,
                    progressLogger,
                    importer
            );
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }

    }

    private final TerminationFlag terminationFlag;
    private final NodeStore nodeStore;
    private final AbstractStorePageCacheScanner<NodeRecord> scanner;
    private final LongSet labels;
    private final int scannerIndex;
    private final ProgressLogger progressLogger;
    private final NodeImporter importer;
    private long propertiesImported;
    private long nodesImported;

    private NodesScanner(
            GraphDatabaseAPI api,
            TerminationFlag terminationFlag,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            LongSet labels,
            int threadIndex,
            ProgressLogger progressLogger,
            NodeImporter importer) {
        super(api);
        this.terminationFlag = terminationFlag;
        this.nodeStore = (NodeStore) scanner.store();
        this.scanner = scanner;
        this.labels = labels;
        this.scannerIndex = threadIndex;
        this.progressLogger = progressLogger;
        this.importer = importer;
    }

    @Override
    public String threadName() {
        return "node-store-scan-" + scannerIndex;
    }

    @Override
    public void accept(KernelTransaction transaction) {
        Read read = transaction.dataRead();
        CursorFactory cursors = transaction.cursors();
        try (AbstractStorePageCacheScanner<NodeRecord>.Cursor cursor = scanner.getCursor()) {
            NodesBatchBuffer batches = new NodesBatchBufferBuilder()
                .store(nodeStore)
                .nodeLabelIds(labels)
                .capacity(cursor.bulkSize())
                .hasLabelInformation(!labels.isEmpty())
                .readProperty(importer.readsProperties())
                .build();
            while (batches.scan(cursor)) {
                terminationFlag.assertRunning();
                long imported = importer.importNodes(batches, read, cursors);
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
