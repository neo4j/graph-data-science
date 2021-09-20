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
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.Stack;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TaskProgressTracker implements ProgressTracker {

    private final Task baseTask;
    private final TaskRegistry taskRegistry;
    private final TaskProgressLogger taskProgressLogger;
    private final Stack<Task> nestedTasks;
    protected Optional<Task> currentTask;

    public TaskProgressTracker(Task baseTask, Log log, int concurrency, TaskRegistryFactory taskRegistryFactory) {
        this(baseTask, new BatchingProgressLogger(log, baseTask, concurrency), taskRegistryFactory);
    }

    public TaskProgressTracker(Task baseTask, ProgressLogger progressLogger, TaskRegistryFactory taskRegistryFactory) {
        this.baseTask = baseTask;
        this.taskRegistry = taskRegistryFactory.newInstance();
        this.taskProgressLogger = new TaskProgressLogger(progressLogger, baseTask);
        this.currentTask = Optional.empty();
        this.nestedTasks = new Stack<>();

        taskRegistry.registerTask(baseTask);
    }

    @Override
    public void setEstimatedResourceFootprint(MemoryRange memoryRangeInBytes, int maxConcurrency) {
        this.baseTask.setEstimatedMemoryRangeInBytes(memoryRangeInBytes);
        this.baseTask.setMaxConcurrency(maxConcurrency);
    }

    @Override
    public void beginSubTask() {
        var nextTask = currentTask.map(task -> {
            nestedTasks.add(task);
            return task.nextSubtask();
        }).orElse(baseTask);
        nextTask.start();
        taskProgressLogger.logBeginSubTask(nextTask, parentTask());
        currentTask = Optional.of(nextTask);
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
    public void endSubTask() {
        var currentTask = requireCurrentTask();
        taskProgressLogger.logEndSubTask(currentTask, parentTask());
        currentTask.finish();
        if (nestedTasks.isEmpty()) {
            this.currentTask = Optional.empty();
            release();
        } else {
            this.currentTask = Optional.of(nestedTasks.pop());
        }
    }

    @Override
    public void endSubTask(String expectedTaskDescription) {
        assertSubTask(expectedTaskDescription);
        endSubTask();
    }

    @Override
    public void logProgress(long value) {
        requireCurrentTask().logProgress(value);
        taskProgressLogger.logProgress(value);
    }

    @Override
    public void logProgress(long value, String messageTemplate) {
        requireCurrentTask().logProgress(value);
        taskProgressLogger.logMessage(formatWithLocale(messageTemplate, value));
    }

    @Override
    public void setVolume(long volume) {
        requireCurrentTask().setVolume(volume);
        taskProgressLogger.reset(volume);
    }

    @Override
    public ProgressLogger progressLogger() {
        return taskProgressLogger.internalProgressLogger();
    }

    @Override
    public void release() {
        taskRegistry.unregisterTask();
        validateTaskFinishedOrCanceled();
    }

    @TestOnly
    public Task currentSubTask() {
        return requireCurrentTask();
    }

    @Nullable
    private Task parentTask() {
        return nestedTasks.isEmpty() ? null : nestedTasks.peek();
    }

    private Task requireCurrentTask() {
        return currentTask.orElseThrow(() -> new IllegalStateException("No more running tasks"));
    }

    private void validateTaskFinishedOrCanceled() {
        if (baseTask.status() == Status.RUNNING) {
            var message = formatWithLocale(
                "Attempted to release algorithm, but task %s is still running",
                baseTask.description()
            );

            // As a bug in logging should not hinder the user in running procedures
            // but only in our tests, we only use an assertion here
            assert false : message;

            progressLogger().getLog().warn(message);
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
