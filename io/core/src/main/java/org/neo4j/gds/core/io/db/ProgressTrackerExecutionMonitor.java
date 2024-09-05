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
package org.neo4j.gds.core.io.db;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public final class ProgressTrackerExecutionMonitor implements Monitor {

    private final long total;
    private final ProgressTracker progressTracker;

    ProgressTrackerExecutionMonitor(
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        this.total = getTotal(graphStore);
        this.progressTracker = progressTracker;
    }

    public static Task progressTask(GraphStore graphStore) {
        return Tasks.leaf(
            GraphStoreToDatabaseExporter.class.getSimpleName(),
            graphStore.nodes().nodeCount() + graphStore.relationshipCount()
        );
    }

    private static long getTotal(GraphStore graphStore) {
        return graphStore.nodeCount() +
            // In block format:
            // Each relationship is sorted and then applied
            // Each relationship is written on both ends (except loops)
            graphStore.relationshipCount() * 4;
    }

    @Override
    public void started() {
        this.progressTracker.beginSubTask();
        this.progressTracker.setVolume(this.total);
    }

    @Override
    public void percentageCompleted(int percentage) {
        long progress = (long) (this.total * (percentage / 100.0));
        this.progressTracker.logProgress(progress);
    }

    @Override
    public void completed(boolean success) {
        this.progressTracker.endSubTask();
    }
}
