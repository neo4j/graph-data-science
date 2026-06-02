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
package org.neo4j.gds.core.utils.progress.tasks;

import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.mem.MemoryRange;

import java.util.Locale;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TaskProgressTracker implements ProgressTracker {
    private static final long UNKNOWN_STEPS = -1;

    private final Stack<Task> nestedTasks = new Stack<>();

    private final Task baseTask;
    private final Consumer<RuntimeException> onError;
    private final TaskProgressLogger taskProgressLogger;
    private final TaskRegistry taskRegistry;

    public Optional<Task> currentTask = Optional.empty();
    public long currentTotalSteps = UNKNOWN_STEPS;
    public double progressLeftOvers = 0;

    public static TaskProgressTracker create(
        LoggerForProgressTracking log,
        Task baseTask,
        Concurrency concurrency,
        JobId jobId,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var taskProgressLogger = TaskProgressLogger.create(log, requestCorrelationId, baseTask, concurrency);

        return create(baseTask, jobId, taskProgressLogger, taskRegistryFactory);
    }

    public static TaskProgressTracker create(
        Task baseTask,
        JobId jobId,
        TaskProgressLogger taskProgressLogger,
        TaskRegistryFactory taskRegistryFactory
    ) {
        var alreadyLoggedOnce = new AtomicBoolean(false);
        Consumer<RuntimeException> onError = error -> {
            if (!alreadyLoggedOnce.get()) {
                taskProgressLogger.logWarning(String.format(Locale.US, ":: %s", error.getMessage()));
                alreadyLoggedOnce.set(true);
            }
        };

        var taskRegistry = taskRegistryFactory.newInstance(jobId);

        return new TaskProgressTracker(
            baseTask,
            onError,
            taskProgressLogger,
            taskRegistry
        );
    }

    private TaskProgressTracker(
        Task baseTask,
        Consumer<RuntimeException> onError,
        TaskProgressLogger taskProgressLogger,
        TaskRegistry taskRegistry
    ) {
        this.baseTask = baseTask;
        this.onError = onError;
        this.taskProgressLogger = taskProgressLogger;
        this.taskRegistry = taskRegistry;
    }

    @Override
    public void setEstimatedResourceFootprint(MemoryRange memoryRangeInBytes) {
        this.baseTask.setEstimatedMemoryRangeInBytes(memoryRangeInBytes);
    }

    @Override
    public void requestedConcurrency(Concurrency concurrency) {
        this.baseTask.setMaxConcurrency(concurrency);
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
        taskProgressLogger.logBeginSubTask(nextTask, parentTask());
        currentTask = Optional.of(nextTask);
        currentTotalSteps = UNKNOWN_STEPS;
        progressLeftOvers = 0;
    }

    @Override
    public void beginSubTask(String expectedTaskDescription) {
        beginSubTask();
        assertSubTask(expectedTaskDescription);
    }

    @Override
    public void beginSubTask(long taskVolume) {
        beginSubTask();
        setVolume(taskVolume);
    }

    @Override
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
    public void logSteps(long steps) { // x
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
    public void beginSubTask(String expectedTaskDescription, long taskVolume) { // x
        beginSubTask();
        assertSubTask(expectedTaskDescription);
        setVolume(taskVolume);
    }

    @Override
    public void endSubTask() {
        requireCurrentTask();
        currentTask.ifPresent(
            task -> {
                taskProgressLogger.logEndSubTask(task, parentTask());
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
    public void endSubTask(String expectedTaskDescription) {
        assertSubTask(expectedTaskDescription);
        endSubTask();
    }

    @Override
    public void onProgress(long value) {
        requireCurrentTask();
        currentTask.ifPresent(task -> {
            task.logProgress(value);
            taskProgressLogger.logProgress(value);
        });
    }

    @Override
    public void onProgress(Function<Long, Long> valueCalculator) {
        requireCurrentTask();

        var currentVolume = (long) currentTask.map(task -> task.getProgress().volume()).orElse(Task.UNKNOWN_VOLUME);

        onProgress(valueCalculator.apply(currentVolume));
    }

    @Override
    public void onProgress(long value, String messageTemplate) {
        requireCurrentTask();
        currentTask.ifPresent(task -> {
            task.logProgress(value);
            taskProgressLogger.logMessage(formatWithLocale(messageTemplate, value));
        });
    }

    /**
     * @deprecated do not use this, it is a hole in our abstraction
     */
    public void setVolume(long volume) {
        requireCurrentTask();
        currentTask.ifPresent(task -> {
            task.setVolume(volume);
            taskProgressLogger.reset(volume);
        });
    }

    @Override
    public void release() {
        validateTaskNotRunning();
        taskRegistry.markCompleted();
        taskProgressLogger.release();
    }

    @Override
    public void endSubTaskWithFailure() {
         currentTask.ifPresent(task -> {
            task.fail();
            taskProgressLogger.logEndSubTaskWithFailure(task, parentTask());
        });

        while (!nestedTasks.isEmpty()) {
            var task = nestedTasks.pop();
            task.fail();
            taskProgressLogger.logEndSubTaskWithFailure(task, parentTask());
        }

        release();
    }

    @Override
    public void endSubTaskWithFailure(String expectedTaskDescription) {
        assertSubTask(expectedTaskDescription);
        endSubTaskWithFailure();
    }

    Task currentSubTask() {
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

            taskProgressLogger.logWarning(message);
        }
    }

    public void assertSubTask(String subTaskSubString) {
        currentTask.ifPresent(task -> {
            var currentTaskDescription = task.description();
            assert currentTaskDescription.contains(subTaskSubString) : formatWithLocale(
                "Expected task name to contain `%s`, but was `%s`",
                subTaskSubString,
                currentTaskDescription
            );
        });
    }

    public Optional<Task> getCurrentTask() {
        return currentTask;
    }
}
