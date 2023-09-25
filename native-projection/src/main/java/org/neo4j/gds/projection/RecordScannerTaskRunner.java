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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.RecordScannerTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

final class RecordScannerTaskRunner {

    private final int threadCount;
    private final RecordScannerTaskFactory recordScannerTaskFactory;

    RecordScannerTaskRunner(int threadCount, RecordScannerTaskFactory recordScannerTaskFactory) {
        this.threadCount = threadCount;
        this.recordScannerTaskFactory = recordScannerTaskFactory;
    }

    ImportResult runImport(ExecutorService executorService) {
        Collection<RecordScannerTask> tasks = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            tasks.add(recordScannerTaskFactory.create(i));
        }

        long start = System.nanoTime();
        ParallelUtil.run(tasks, executorService);

        ParallelUtil.run(recordScannerTaskFactory.adjacencyListBuilderTasks(), executorService);
        long elapsed = System.nanoTime() - start;

        long importedRecords = 0L;
        long importedProperties = 0L;
        for (RecordScannerTask task : tasks) {
            importedRecords += task.recordsImported();
            importedProperties += task.propertiesImported();
        }

        return ImmutableImportResult
            .builder()
            .importedRecords(importedRecords)
            .importedProperties(importedProperties)
            .durationNanos(elapsed)
            .build();
    }

    @ValueClass
    interface ImportResult {
        long durationNanos();

        long importedRecords();

        long importedProperties();
    }

    public interface RecordScannerTaskFactory {

        RecordScannerTask create(int taskIndex);

        // TODO: only necessary for relationships, can we move it somewhere else?
        Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks();
    }
}
