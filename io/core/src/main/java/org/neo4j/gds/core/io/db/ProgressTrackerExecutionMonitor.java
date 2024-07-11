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
import org.neo4j.gds.compat.batchimport.ExecutionMonitor;
import org.neo4j.gds.compat.batchimport.Monitor;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.batchimport.staging.StageExecution;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.Keys;

import java.util.concurrent.TimeUnit;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ProgressTrackerExecutionMonitor implements ExecutionMonitor {

    private final long intervalMillis;
    private final long totalNumberOfBatches;
    private long prevDoneBatches;
    private long totalReportedBatches;
    private final long total;
    private final ProgressTracker progressTracker;

    ProgressTrackerExecutionMonitor(
        GraphStore graphStore,
        ProgressTracker progressTracker,
        long batchSize
    ) {
        long highNodeId = graphStore.nodeCount();
        long highRelationshipId = graphStore.relationshipCount();
        this.intervalMillis = TimeUnit.SECONDS.toMillis(1);
        this.totalNumberOfBatches = highNodeId / batchSize * 3L + highRelationshipId / batchSize * 4L;
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
    public void initialize(DependencyResolver dependencyResolver) {
        this.progressTracker.beginSubTask();
        this.progressTracker.setVolume(this.totalNumberOfBatches);
    }

    @Override
    public void start(StageExecution execution) {
        this.prevDoneBatches = 0L;
        progressTracker.logInfo(formatWithLocale("%s :: Start", execution.getStageName()));
    }

    @Override
    public void end(StageExecution execution, long totalTimeMillis) {
        progressTracker.logInfo(formatWithLocale("%s :: Finished", execution.getStageName()));
    }

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        this.progress(this.totalNumberOfBatches - this.totalReportedBatches);
        this.progressTracker.endSubTask();
        this.progressTracker.logInfo(additionalInformation);
    }

    @Override
    public long checkIntervalMillis() {
        return this.intervalMillis;
    }

    @Override
    public void check(StageExecution execution) {
        long doneBatches = doneBatches(execution);
        long diff = doneBatches - this.prevDoneBatches;
        this.prevDoneBatches = doneBatches;
        if (diff > 0L) {
            this.totalReportedBatches += diff;
            this.progress(diff);
        }

    }

    private static long doneBatches(StageExecution execution) {
        Step<?> lastStep = null;
        for (Step<?> step : execution.steps()) {
            lastStep = step;
        }
        if (lastStep == null) {
            return 0;
        }

        return lastStep.stats().stat(Keys.done_batches).asLong();
    }

    private void progress(long progress) {
        this.progressTracker.logProgress(progress);
    }

    @Override
    public Monitor toMonitor() {
        return new ProgressMonitor(this.total, progressTracker);
    }

    private static final class ProgressMonitor implements Monitor {

        private final long total;
        private final ProgressTracker progressTracker;

        private ProgressMonitor(long total, ProgressTracker progressTracker) {
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
