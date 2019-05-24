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

import org.neo4j.graphalgo.core.utils.ParallelUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

final class ImportingThreadPool {

    interface CreateScanner {
        RecordScanner create(int index);

        Collection<Runnable> flushTasks();
    }

    static CreateScanner createEmptyScanner() {
        return NoRecordsScanner.INSTANCE;
    }

    private final int numberOfThreads;
    private final CreateScanner createScanner;

    ImportingThreadPool(
            final int numberOfThreads,
            final CreateScanner createScanner) {
        this.numberOfThreads = numberOfThreads;
        this.createScanner = createScanner;
    }

    ImportResult run(ExecutorService pool) {
        Collection<RecordScanner> tasks = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            tasks.add(createScanner.create(i));
        }

        long scannerStart = System.nanoTime();
        ParallelUtil.run(tasks, pool);
        ParallelUtil.run(createScanner.flushTasks(), pool);
        long took = System.nanoTime() - scannerStart;
        long imported = 0L;
        for (RecordScanner task : tasks) {
            imported += task.recordsImported();
        }

        return new ImportResult(took, imported);
    }

    static final class ImportResult {
        final long tookNanos;
        final long recordsImported;

        ImportResult(long tookNanos, long recordsImported) {
            this.tookNanos = tookNanos;
            this.recordsImported = recordsImported;
        }
    }

    private static final class NoRecordsScanner implements RecordScanner, ImportingThreadPool.CreateScanner {
        private static final NoRecordsScanner INSTANCE = new NoRecordsScanner();

        @Override
        public long recordsImported() {
            return 0L;
        }

        @Override
        public void run() {
        }

        @Override
        public RecordScanner create(final int index) {
            return this;
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }
    }
}
