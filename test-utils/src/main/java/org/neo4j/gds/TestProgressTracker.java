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
package org.neo4j.gds;

import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.mem.MemoryRange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class TestProgressTracker implements ProgressTracker {
    private final List<AtomicLong> progresses = new ArrayList<>();

    private final TaskProgressTracker delegate;

    public static TestProgressTracker create(
        Task baseTask,
        LoggerForProgressTracking log,
        Concurrency concurrency,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var delegate = TaskProgressTracker.create(
            baseTask,
            log,
            concurrency,
            new JobId(),
            PlainSimpleRequestCorrelationId.create(),
            taskRegistryFactory,
            UserLogRegistry.EMPTY
        );

        return new TestProgressTracker(delegate);
    }

    private TestProgressTracker(
        TaskProgressTracker delegate
    ) {
        this.delegate = delegate;
    }

    public List<AtomicLong> getProgresses() {
        return progresses;
    }

    @Override
    public void logProgress(long progress) {
        progresses.getLast().addAndGet(progress);
        delegate.logProgress(progress);
    }

    @Override
    public void logProgress(long value, String messageTemplate) {
        delegate.logProgress(value, messageTemplate);
    }

    @Override
    public void setEstimatedResourceFootprint(MemoryRange memoryEstimationInBytes) {
        delegate.setEstimatedResourceFootprint(memoryEstimationInBytes);
    }

    @Override
    public void requestedConcurrency(Concurrency concurrency) {
        delegate.requestedConcurrency(concurrency);
    }

    @Override
    public void beginSubTask() {
        delegate.beginSubTask();
        delegate.getCurrentTask().ifPresent(__ -> progresses.add(new AtomicLong()));
    }

    @Override
    public void beginSubTask(long taskVolume) {
        beginSubTask();
        setVolume(taskVolume);
    }

    @Override
    public void beginSubTask(String expectedTaskDescription) {
        beginSubTask();
        delegate.assertSubTask(expectedTaskDescription);
    }

    @Override
    public void beginSubTask(String expectedTaskDescription, long taskVolume) {
        beginSubTask();
        delegate.assertSubTask(expectedTaskDescription);
        setVolume(taskVolume);
    }

    @Override
    public void endSubTask() {
        delegate.endSubTask();
    }

    @Override
    public void endSubTask(String expectedTaskDescription) {
        delegate.endSubTask(expectedTaskDescription);
    }

    @Override
    public void endSubTaskWithFailure() {
        delegate.endSubTaskWithFailure();
    }

    @Override
    public void endSubTaskWithFailure(String expectedTaskDescription) {
        delegate.endSubTaskWithFailure(expectedTaskDescription);
    }

    @Override
    public void setVolume(long volume) {
        delegate.setVolume(volume);
        progresses.add(new AtomicLong());
    }

    @Override
    public long currentVolume() {
        return delegate.currentVolume();
    }

    @Override
    public void logDebug(Supplier<String> messageSupplier) {
        delegate.logDebug(messageSupplier);
    }

    @Override
    public void logMessage(LogLevel level, String message) {
        delegate.logMessage(level, message);
    }

    @Override
    public void release() {
        delegate.release();
    }

    @Override
    public void setSteps(long steps) {
        delegate.setSteps(steps);
    }

    @Override
    public void logSteps(long steps) {
        delegate.requireCurrentTask();
        delegate.currentTask.ifPresent(task -> {
            long volume = task.getProgress().volume();
            double progress = steps * volume / (double) delegate.currentTotalSteps + delegate.progressLeftOvers;
            long longProgress = (long) progress;
            delegate.progressLeftOvers = progress - longProgress;
            logProgress(longProgress);
        });
    }
}
