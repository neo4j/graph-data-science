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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.core.concurrency.ParallelUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

public final class InternalImporter {

    public interface RecordScannerTaskFactory {

        RecordScannerTask create(int taskIndex);

        void prepareFlushTasks();

        Collection<Runnable> flushTasks();
    }

    public static RecordScannerTaskFactory createEmptyScanner() {
        return NoRecordsScannerTaskFactory.INSTANCE;
    }

    private final int numberOfThreads;
    private final RecordScannerTaskFactory recordScannerBuilder;

    InternalImporter(int numberOfThreads, RecordScannerTaskFactory recordScannerBuilder) {
        this.numberOfThreads = numberOfThreads;
        this.recordScannerBuilder = recordScannerBuilder;
    }

    ImportResult runImport(ExecutorService pool) {
        Collection<RecordScannerTask> tasks = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            tasks.add(recordScannerBuilder.create(i));
        }

        long scannerStart = System.nanoTime();
        ParallelUtil.run(tasks, pool);
        recordScannerBuilder.prepareFlushTasks();
        ParallelUtil.run(recordScannerBuilder.flushTasks(), pool);
        long took = System.nanoTime() - scannerStart;
        long importedRecords = 0L;
        long importedProperties = 0L;
        for (RecordScannerTask task : tasks) {
            importedRecords += task.recordsImported();
            importedProperties += task.propertiesImported();
        }

        return new ImportResult(took, importedRecords, importedProperties);
    }

    static final class ImportResult {
        final long tookNanos;
        final long recordsImported;
        final long propertiesImported;

        ImportResult(long tookNanos, long recordsImported, long propertiesImported) {
            this.tookNanos = tookNanos;
            this.recordsImported = recordsImported;
            this.propertiesImported = propertiesImported;
        }
    }

    private static final class NoRecordsScannerTaskFactory implements RecordScannerTask, RecordScannerTaskFactory {
        private static final NoRecordsScannerTaskFactory INSTANCE = new NoRecordsScannerTaskFactory();

        @Override
        public long propertiesImported() {
            return 0;
        }

        @Override
        public long recordsImported() {
            return 0L;
        }

        @Override
        public void run() {
        }

        @Override
        public RecordScannerTask create(final int taskIndex) {
            return this;
        }

        @Override
        public void prepareFlushTasks() {
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }
    }
}
