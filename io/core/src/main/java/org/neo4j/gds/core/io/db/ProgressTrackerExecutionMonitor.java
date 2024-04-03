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

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.CompatExecutionMonitor;
import org.neo4j.gds.compat.CompatMonitor;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageExecution;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;


public final class ProgressTrackerExecutionMonitor
    extends CoarseBoundedProgressExecutionMonitor
    implements CompatExecutionMonitor {

    private final long total;
    private final ProgressTracker progressTracker;

    public static Task progressTask(GraphStore graphStore) {
        return Tasks.leaf(
            GraphStoreToDatabaseExporter.class.getSimpleName(),
            graphStore.nodes().nodeCount() + graphStore.relationshipCount()
        );
    }

    ProgressTrackerExecutionMonitor(
        GraphStore graphStore,
        ProgressTracker progressTracker,
        Configuration config
    ) {
        super(graphStore.nodeCount(), graphStore.relationshipCount(), config);
        this.total = getTotal(graphStore);
        this.progressTracker = progressTracker;
    }

    private static long getTotal(GraphStore graphStore) {
        return graphStore.nodeCount() +
            // In block format:
            // Each relationship is sorted and then applied
            // Each relationship is written on both ends (except loops)
            graphStore.relationshipCount() * 4;
    }

    @Override
    public void initialize(DependencyResolver dependencyResolver) {
        this.progressTracker.beginSubTask();
        this.progressTracker.setVolume(this.total());
    }

    @Override
    public void start(StageExecution execution) {
        super.start(execution);
        progressTracker.logInfo(formatWithLocale("%s :: Start", execution.getStageName()));
    }

    @Override
    public void end(StageExecution execution, long totalTimeMillis) {
        super.end(execution, totalTimeMillis);
        progressTracker.logInfo(formatWithLocale("%s :: Finished", execution.getStageName()));
    }

    @Override
    protected void progress(long progress) {
        this.progressTracker.logProgress(progress);
    }

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        super.done(successful, totalTimeMillis, additionalInformation);
        this.progressTracker.endSubTask();
        this.progressTracker.logInfo(additionalInformation);
    }

    @Override
    public CompatMonitor toCompatMonitor() {
        return new Monitor(this.total, progressTracker);
    }

    private static final class Monitor implements CompatMonitor {

        private final long total;
        private final ProgressTracker progressTracker;

        private Monitor(long total, ProgressTracker progressTracker) {
            this.total = total;
            this.progressTracker = progressTracker;
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
}
