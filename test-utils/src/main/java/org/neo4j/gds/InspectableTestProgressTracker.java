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
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.Progress;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.mem.MemoryRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.core.utils.progress.tasks.Task.UNKNOWN_VOLUME;

public final class InspectableTestProgressTracker implements ProgressTracker {
    private final List<Optional<Progress>> progressHistory = new ArrayList<>();

    private final ProgressTracker delegate;
    private final TaskStore taskStore;
    private final JobId jobId;
    private final String userName;

    private InspectableTestProgressTracker(
        ProgressTracker delegate,
        TaskStore taskStore,
        JobId jobId,
        String userName
    ) {
        this.delegate = delegate;
        this.taskStore = taskStore;
        this.jobId = jobId;
        this.userName = userName;
    }

    public static InspectableTestProgressTracker create(
        Task baseTask,
        String userName,
        JobId jobId,
        TaskStore taskStore,
        LoggerForProgressTracking log
    ) {
        var delegate = TaskProgressTracker.create(
            baseTask,
            log,
            new Concurrency(1),
            jobId,
            PlainSimpleRequestCorrelationId.create(),
            TaskRegistryFactory.local(userName, taskStore),
            UserLogRegistry.EMPTY
        );

        return new InspectableTestProgressTracker(delegate, taskStore, jobId, userName);
    }

    @Override
    public void logProgress(long progress) {
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
        progressHistory.add(taskStore.query(userName, jobId).map(userTask -> userTask.task().getProgress()));
    }

    @Override
    public void beginSubTask(long taskVolume) {
        delegate.beginSubTask(taskVolume);
    }

    @Override
    public void beginSubTask(String expectedTaskDescription) {
        delegate.beginSubTask(expectedTaskDescription);
    }

    @Override
    public void beginSubTask(String expectedTaskDescription, long taskVolume) {
        delegate.beginSubTask(expectedTaskDescription, taskVolume);
    }

    @Override
    public void endSubTask() {
        delegate.endSubTask();
        progressHistory.add(taskStore.query(userName, jobId).map(userTask -> userTask.task().getProgress()));
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
        delegate.logSteps(steps);
    }

    public void assertValidProgressEvolution() {
        assertThat(progressHistory).isNotEmpty();
        assertThat(progressHistory.getFirst()).isPresent();
        var previousProgress = progressHistory.getFirst().get();
        var initialVolume = previousProgress.volume();
        assertThat(initialVolume).isNotEqualTo(UNKNOWN_VOLUME);
        assertThat(previousProgress.progress()).isEqualTo(0);
        for (Optional<Progress> maybeProgress : progressHistory.subList(1, progressHistory.size())) {
            if (maybeProgress.isPresent()) {
                var progress = maybeProgress.get();
                assertThat(progress.volume()).isEqualTo(initialVolume);
                assertThat(progress.progress()).isGreaterThanOrEqualTo(previousProgress.progress());
                previousProgress = progress;
            }
        }
        assertThat(previousProgress.progress()).isEqualTo(previousProgress.volume());
    }
}
