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
import org.neo4j.gds.progress.logging.ProgressLogger;
import org.neo4j.gds.progress.registration.TaskRegistry;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.tasks.Status;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.logging.TaskProgressLogger;

import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TaskProgressTracker implements ProgressTracker {
    private static final long UNKNOWN_STEPS = -1;

    private final Stack<Task> nestedTasks = new Stack<>();

    private final Log log;
    private final Task baseTask;
    private final Consumer<RuntimeException> onError;
    private final ProgressLogger progressLogger;
    private final TaskRegistry taskRegistry;

    public Optional<Task> currentTask = Optional.empty();
    public long currentTotalSteps = UNKNOWN_STEPS;
    public double progressLeftOvers = 0;

    public static TaskProgressTracker create(
        Log log,
        LoggerForProgressTracking loggerForProgressTracking,
        Task baseTask,
        Concurrency concurrency,
        JobId jobId,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var taskProgressLogger = TaskProgressLogger.create(loggerForProgressTracking, requestCorrelationId, baseTask, concurrency);

        return create(log, baseTask, jobId, taskProgressLogger, taskRegistryFactory);
    }

    public static TaskProgressTracker create(
        Log log,
        Task baseTask,
        JobId jobId,
        ProgressLogger progressLogger,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var alreadyLoggedOnce = new AtomicBoolean(false);
        Consumer<RuntimeException> onError = error -> {
            if (!alreadyLoggedOnce.get()) {
                log.warn("Progress logging out of sync with declared task tree", error.getMessage());
                alreadyLoggedOnce.set(true);
            }
        };

        var taskRegistry = taskRegistryFactory.newInstance(jobId);

        return new TaskProgressTracker(
            log,
            baseTask,
            onError,
            progressLogger,
            taskRegistry
        );
    }

    private TaskProgressTracker(
        Log log,
        Task baseTask,
        Consumer<RuntimeException> onError,
        ProgressLogger progressLogger,
        TaskRegistry taskRegistry
    ) {
        this.log = log;
        this.baseTask = baseTask;
        this.onError = onError;
        this.progressLogger = progressLogger;
        this.taskRegistry = taskRegistry;
    }

    @Override
    public void beginSubTask() {
        registerBaseTask();
        var nextTask = currentTask.map(task -> {
            nestedTasks.add(task);
            try {
                return task.nextSubtask();
            } catch (IllegalStateException e) {
                onError.accept(e);
            }
            return baseTask;
        }).orElse(baseTask);
        nextTask.start();
        progressLogger.logBeginSubTask(nextTask, parentTask());
        currentTask = Optional.of(nextTask);
        currentTotalSteps = UNKNOWN_STEPS;
        progressLeftOvers = 0;
    }

    @Override
    public void beginSubTask(long taskVolume) {
        beginSubTask();
        setVolume(taskVolume);
    }

    @Override
    public void beginSubTaskWithSteps(long numberOfSteps) {
        beginSubTask();
        setSteps(numberOfSteps);
    }

    /**
     * @deprecated do not use this, it is a hole in our abstraction
     */
    @Deprecated
    public void setSteps(long steps) {
        if (steps <= 0) {
            throw new IllegalStateException(formatWithLocale(
                "Total steps for task must be at least 1 but was %d",
                steps
            ));
        }
        currentTotalSteps = steps;
    }

    @Override
    public void onSteps(long steps) { // x
        requireCurrentTask();
        currentTask.ifPresent(task -> {
            long volume = task.getProgress().volume();
            double progress = steps * volume / (double) currentTotalSteps + progressLeftOvers;
            long longProgress = (long) progress;
            progressLeftOvers = progress - longProgress;
            onProgress(longProgress);
        });
    }

    @Override
    public void endSubTask() {
        requireCurrentTask();
        currentTask.ifPresent(
            task -> {
                progressLogger.logEndSubTask(task, parentTask());
                task.finish();
                if (nestedTasks.isEmpty()) {
                    this.currentTask = Optional.empty();
                    release();
                } else {
                    this.currentTask = Optional.of(nestedTasks.pop());
                }
            }
        );

    }

    @Override
    public void onProgress(long value) {
        requireCurrentTask();
        currentTask.ifPresent(task -> {
            task.logProgress(value);
            progressLogger.logProgress(value);
        });
    }

    @Override
    public void onProgress(Function<Long, Long> valueCalculator) {
        requireCurrentTask();

        var currentVolume = (long) currentTask.map(task -> task.getProgress().volume()).orElse(Task.UNKNOWN_VOLUME);

        onProgress(valueCalculator.apply(currentVolume));
    }

    /**
     * @deprecated do not use this, it is a hole in our abstraction
     */
    @Deprecated
    public void setVolume(long volume) {
        requireCurrentTask();
        currentTask.ifPresent(task -> {
            task.setVolume(volume);
            progressLogger.reset(volume);
        });
    }

    @Override
    public void release() {
        validateTaskNotRunning();
        taskRegistry.markCompleted();
        progressLogger.release();
    }

    @Override
    public void endSubTaskWithFailure() {
         currentTask.ifPresent(task -> {
            task.fail();
            progressLogger.logEndSubTaskWithFailure(task, parentTask());
        });

        while (!nestedTasks.isEmpty()) {
            var task = nestedTasks.pop();
            task.fail();
            progressLogger.logEndSubTaskWithFailure(task, parentTask());
        }

        release();
    }

    public Task currentSubTask() {
        requireCurrentTask();
        return currentTask.orElseThrow();
    }

    private Task parentTask() {
        return nestedTasks.isEmpty() ? null : nestedTasks.peek();
    }

    private void registerBaseTask() {
        if (!taskRegistry.containsTask(baseTask)) {
            taskRegistry.registerTask(baseTask);
        }
    }

    public void requireCurrentTask() {
        if (currentTask.isEmpty()) {
            onError.accept(new IllegalStateException(
                "Tried to log progress, but there are no running tasks being tracked"));
        }
    }

    private void validateTaskNotRunning() {
        if (baseTask.status() == Status.RUNNING) {
            var message = formatWithLocale(
                "Attempted to release algorithm, but task %s is still running",
                baseTask.description()
            );

            log.warn(message);
        }
    }

    public Optional<Task> getCurrentTask() {
        return currentTask;
    }
}
