/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.ImportProgress;
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
            int label,
            ImportProgress progress,
            NodeImporter importer,
            TerminationFlag terminationFlag) {
        return new NodesScanner.Creator(
                api,
                scanner,
                label,
                progress,
                importer,
                terminationFlag);
    }

    static final class Creator implements InternalImporter.CreateScanner {
        private final GraphDatabaseAPI api;
        private final AbstractStorePageCacheScanner<NodeRecord> scanner;
        private final int label;
        private final ImportProgress progress;
        private final NodeImporter importer;
        private final TerminationFlag terminationFlag;

        Creator(
                GraphDatabaseAPI api,
                AbstractStorePageCacheScanner<NodeRecord> scanner,
                int label,
                ImportProgress progress,
                NodeImporter importer, 
                TerminationFlag terminationFlag) {
            this.api = api;
            this.scanner = scanner;
            this.label = label;
            this.progress = progress;
            this.importer = importer;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RecordScanner create(int index) {
            return new NodesScanner(
                    api,
                    terminationFlag,
                    scanner,
                    label,
                    index,
                    progress,
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
    private final int label;
    private final int scannerIndex;
    private final ImportProgress progress;
    private final NodeImporter importer;
    private long propertiesImported;
    private long nodesImported;

    private NodesScanner(
            GraphDatabaseAPI api,
            TerminationFlag terminationFlag,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            int label,
            int threadIndex,
            ImportProgress progress,
            NodeImporter importer) {
        super(api);
        this.terminationFlag = terminationFlag;
        this.nodeStore = (NodeStore) scanner.store();
        this.scanner = scanner;
        this.label = label;
        this.scannerIndex = threadIndex;
        this.progress = progress;
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
            NodesBatchBuffer batches = new NodesBatchBuffer(
                    nodeStore,
                    label,
                    cursor.bulkSize(),
                    importer.readsProperties());
            ImportProgress progress = this.progress;
            while (batches.scan(cursor)) {
                terminationFlag.assertRunning();
                long imported = importer.importNodes(batches, read, cursors);
                int batchImportedNodes = RawValues.getHead(imported);
                int batchImportedProperties = RawValues.getTail(imported);
                progress.nodesImported(batchImportedNodes);
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
