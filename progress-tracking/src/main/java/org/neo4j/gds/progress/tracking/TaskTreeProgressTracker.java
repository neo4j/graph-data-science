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
package org.neo4j.gds.progress.tracking;

import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.tasks.LeafTask;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.TaskVisitor;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.logging.TaskProgressLogger;

import java.util.function.Function;

public final class TaskTreeProgressTracker implements ProgressTracker {
    private final TaskProgressTracker delegate;

    private TaskTreeProgressTracker(TaskProgressTracker delegate) {this.delegate = delegate;}

    public static TaskTreeProgressTracker create(
        Log log,
        LoggerForProgressTracking loggerForProgressTracking,
        Task baseTask,
        Concurrency concurrency,
        JobId jobId,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var taskVisitor = new PassThroughTaskVisitor();
        var taskProgressLogger = TaskProgressLogger.create(
            loggerForProgressTracking,
            requestCorrelationId,
            baseTask,
            concurrency,
            taskVisitor
        );

        var taskRegistry = taskRegistryFactory.newInstance(jobId);

        var delegate = TaskProgressTracker.create(
            log,
            baseTask,
            taskProgressLogger,
            taskRegistry
        );

        return new TaskTreeProgressTracker(delegate);
    }

    @Override
    public void onSteps(long steps) {
        // NOOP
    }

    @Override
    public void beginSubTask() {
        delegate.beginSubTask();
    }

    @Override
    public void beginSubTask(long taskVolume) {
        delegate.beginSubTask(taskVolume);
    }

    @Override
    public void beginSubTaskWithSteps(long numberOfSteps) {
        delegate.beginSubTaskWithSteps(numberOfSteps);
    }

    @Override
    public void endSubTask() {
        delegate.endSubTask();
    }

    @Override
    public void endSubTaskWithFailure() {
        delegate.endSubTaskWithFailure();
    }

    @Override
    public void onProgress(long value) {
        // NOOP
    }

    @Override
    public void onProgress(Function<Long, Long> valueCalculator) {
        // NOOP
    }

    @Override
    public void release() {
        delegate.release();
    }

    private static class PassThroughTaskVisitor implements TaskVisitor {
        @Override
        public void visitLeafTask(LeafTask leafTask) {
            // NOOP --> just pass through
        }
    }
}
