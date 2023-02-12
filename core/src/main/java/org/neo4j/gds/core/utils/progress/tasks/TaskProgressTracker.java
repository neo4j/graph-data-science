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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.neo4j.gds.core.utils.progress.tasks.Task.UNKNOWN_VOLUME;
import static org.neo4j.gds.utils.GdsFeatureToggles.THROW_WHEN_USING_PROGRESS_TRACKER_WITHOUT_TASKS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TaskProgressTracker implements ProgressTracker {

    private static final long UNKNOWN_STEPS = -1;

    private final Task baseTask;
    private final TaskRegistry taskRegistry;
    private final UserLogRegistry userLogRegistry;
    private final TaskProgressLogger taskProgressLogger;
    private final Stack<Task> nestedTasks;
    protected Optional<Task> currentTask;
    private long currentTotalSteps;
    private double progressLeftOvers;

    private final Runnable onError;

    public TaskProgressTracker(Task baseTask, Log log, int concurrency, TaskRegistryFactory taskRegistryFactory) {
        this(baseTask, log, concurrency, new JobId(), taskRegistryFactory, EmptyUserLogRegistryFactory.INSTANCE);
    }

    public TaskProgressTracker(
        Task baseTask,
        Log log,
        int concurrency,
        JobId jobId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        this(baseTask, jobId, taskRegistryFactory, new TaskProgressLogger(log, baseTask, concurrency), userLogRegistryFactory);
    }

    protected TaskProgressTracker(
        Task baseTask,
        JobId jobId,
        TaskRegistryFactory taskRegistryFactory,
        TaskProgressLogger taskProgressLogger,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        this.baseTask = baseTask;
        this.taskRegistry = taskRegistryFactory.newInstance(jobId);
        this.taskProgressLogger = taskProgressLogger;
        this.currentTask = Optional.empty();
        this.currentTotalSteps = UNKNOWN_STEPS;
        this.progressLeftOvers = 0;
        this.nestedTasks = new Stack<>();
        this.userLogRegistry = userLogRegistryFactory.newInstance();
        if (THROW_WHEN_USING_PROGRESS_TRACKER_WITHOUT_TASKS.isEnabled()) {
            this.onError = () -> {
                throw new IllegalStateException("Tried to log progress, but there are no running tasks being tracked");
            };
        } else {
            AtomicBoolean didLog = new AtomicBoolean(false);
            this.onError = () -> {
                if (!didLog.get()) {
                    taskProgressLogger.logWarning(":: Tried to log progress, but there are no running tasks being tracked");
                    didLog.set(true);
                }
            };
        }
    }

    @Override
    public void setEstimatedResourceFootprint(MemoryRange memoryRangeInBytes, int maxConcurrency) {
        this.baseTask.setEstimatedMemoryRangeInBytes(memoryRangeInBytes);
        this.baseTask.setMaxConcurrency(maxConcurrency);
    }

    @Override
    public void beginSubTask() {
        registerBaseTask();
        var nextTask = currentTask.map(task -> {
            nestedTasks.add(task);
            return task.nextSubtask();
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
    public void logSteps(long steps) {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            long volume = currentTask.get().getProgress().volume();
            double progress = steps * volume / (double) currentTotalSteps + progressLeftOvers;
            long longProgress = (long) progress;
            progressLeftOvers = progress - longProgress;
            logProgress(longProgress);
        }
    }

    @Override
    public void beginSubTask(String expectedTaskDescription, long taskVolume) {
        beginSubTask();
        assertSubTask(expectedTaskDescription);
        setVolume(taskVolume);
    }

    @Override
    public void endSubTask() {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            taskProgressLogger.logEndSubTask(currentTask.get(), parentTask());
            currentTask.get().finish();
            if (nestedTasks.isEmpty()) {
                this.currentTask = Optional.empty();
                release();
            } else {
                this.currentTask = Optional.of(nestedTasks.pop());
            }
        }
    }

    @Override
    public void endSubTask(String expectedTaskDescription) {
        assertSubTask(expectedTaskDescription);
        endSubTask();
    }

    @Override
    public void logProgress(long value) {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            currentTask.get().logProgress(value);
            taskProgressLogger.logProgress(value);
        }
    }

    @Override
    public void logProgress(long value, String messageTemplate) {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            currentTask.get().logProgress(value);
            taskProgressLogger.logMessage(formatWithLocale(messageTemplate, value));
        }
    }

    @Override
    public void setVolume(long volume) {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            currentTask.get().setVolume(volume);
            taskProgressLogger.reset(volume);
        }
    }

    @Override
    public long currentVolume() {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            return currentTask.get().getProgress().volume();
        } else {
            return UNKNOWN_VOLUME;
        }
    }

    @Override
    public void logMessage(LogLevel level, String message) {
        switch (level) {
            case WARNING:
                userLogRegistry.addWarningToLog(baseTask, message);
                taskProgressLogger.logWarning(":: " + message);
                break;
            case INFO:
                taskProgressLogger.logMessage(":: " + message);
                break;
            case DEBUG:
                taskProgressLogger.logDebug(":: " + message);
                break;
            default:
                throw new IllegalStateException("Unknown log level " + level);
        }
    }

    @Override
    public void release() {
        taskRegistry.unregisterTask();
        taskProgressLogger.release();
        validateTaskNotRunning();
    }

    @Override
    public void endSubTaskWithFailure() {
        requireCurrentTask();
        if (currentTask.isPresent()) {
            currentTask.get().fail();
            taskProgressLogger.logEndSubTaskWithFailure(currentTask.get(), parentTask());

            if (nestedTasks.isEmpty()) {
                this.currentTask = Optional.empty();
                release();
            } else {
                this.currentTask = Optional.of(nestedTasks.pop());
                endSubTaskWithFailure();
            }
        }
    }

    @Override
    public void endSubTaskWithFailure(String expectedTaskDescription) {
        assertSubTask(expectedTaskDescription);
        endSubTaskWithFailure();
    }

    @TestOnly
    Task currentSubTask() {
        requireCurrentTask();
        return currentTask.orElseThrow();
    }

    @Nullable
    private Task parentTask() {
        return nestedTasks.isEmpty() ? null : nestedTasks.peek();
    }

    private void registerBaseTask() {
        if (!taskRegistry.containsTask(baseTask)) {
            taskRegistry.registerTask(baseTask);
        }
    }

    private void requireCurrentTask() {
        if (currentTask.isEmpty()) {
            onError.run();
        }
    }

    private void validateTaskNotRunning() {
        if (baseTask.status() == Status.RUNNING) {
            var message = formatWithLocale(
                "Attempted to release algorithm, but task %s is still running",
                baseTask.description()
            );

            // As a bug in logging should not hinder the user in running procedures
            // but only in our tests, we only use an assertion here
            assert false : message;

            taskProgressLogger.logWarning(message);
        }
    }

    private void assertSubTask(String subTaskSubString) {
        if (currentTask.isPresent()) {
            var currentTaskDescription = currentTask.get().description();
            assert currentTaskDescription.contains(subTaskSubString) : formatWithLocale(
                "Expected task name to contain `%s`, but was `%s`",
                subTaskSubString,
                currentTaskDescription
            );
        }
    }
}
