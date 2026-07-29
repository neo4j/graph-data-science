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

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.registration.TaskStore;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.tasks.Progress;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tracking.TaskProgressTracker;
import org.neo4j.gds.logging.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.progress.tasks.Task.UNKNOWN_VOLUME;

public final class InspectableTestProgressTracker implements ProgressTracker {
    private final List<Optional<Progress>> progressHistory = new ArrayList<>();

    private final ProgressTracker delegate;
    private final TaskStore taskStore;
    private final User user;
    private final JobId jobId;

    private InspectableTestProgressTracker(
        ProgressTracker delegate,
        TaskStore taskStore,
        User user,
        JobId jobId
    ) {
        this.delegate = delegate;
        this.taskStore = taskStore;
        this.user = user;
        this.jobId = jobId;
    }

    public static InspectableTestProgressTracker create(
        Log log,
        LoggerForProgressTracking loggerForProgressTracking,
        TaskStore taskStore,
        Task baseTask,
        User user,
        JobId jobId
    ) {
        var taskRegistryFactory = TaskRegistryFactory.local(log, taskStore, user);
        var taskRegistry = taskRegistryFactory.newInstance(jobId);

        var delegate = TaskProgressTracker.create(
            log,
            loggerForProgressTracking,
            baseTask,
            new Concurrency(1),
            PlainSimpleRequestCorrelationId.create(),
            taskRegistry
        );

        return new InspectableTestProgressTracker(delegate, taskStore, user, jobId);
    }

    @Override
    public void onProgress(long progress) {
        delegate.onProgress(progress);
    }

    @Override
    public void onProgress(Function<Long, Long> valueCalculator) {
        delegate.onProgress(valueCalculator);
    }

    @Override
    public void beginSubTask() {
        delegate.beginSubTask();

        registerProgress();
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

        registerProgress();
    }

    @Override
    public void endSubTaskWithFailure() {
        delegate.endSubTaskWithFailure();
    }

    @Override
    public void release() {
        delegate.release();
    }

    @Override
    public void onSteps(long steps) {
        delegate.onSteps(steps);
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

    private void registerProgress() {
        var tasks = taskStore.lookup(user, jobId);

        tasks.forEach(storedTask -> progressHistory.add(Optional.of(storedTask.task().getProgress())));
    }
}
