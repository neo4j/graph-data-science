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
package org.neo4j.gds.progress.logging;

import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.tasks.IterativeTask;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.TaskVisitor;

import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TaskProgressLogger implements ProgressLogger {
    private static final String TASK_SEPARATOR = " :: ";

    private final BatchingProgressLogger batchingProgressLogger;
    private final Task baseTask;
    private final TaskVisitor loggingLeafTaskVisitor;

    private TaskProgressLogger(
        BatchingProgressLogger batchingProgressLogger,
        Task baseTask,
        TaskVisitor loggingLeafTaskVisitor
    ) {
        this.batchingProgressLogger = batchingProgressLogger;
        this.baseTask = baseTask;
        this.loggingLeafTaskVisitor = loggingLeafTaskVisitor;
    }

    public static TaskProgressLogger create(
        LoggerForProgressTracking log,
        RequestCorrelationId requestCorrelationId,
        Task baseTask,
        Concurrency concurrency
    ) {
        var batchingProgressLogger = BatchingProgressLogger.create(log, requestCorrelationId, baseTask, concurrency);
        var loggingLeafTaskVisitor = new LoggingLeafTaskVisitor(batchingProgressLogger);

        return new TaskProgressLogger(batchingProgressLogger, baseTask, loggingLeafTaskVisitor);
    }

    public static TaskProgressLogger create(
        LoggerForProgressTracking log,
        RequestCorrelationId requestCorrelationId,
        Task baseTask,
        Concurrency concurrency,
        TaskVisitor leafTaskVisitor
    ) {
        var batchingProgressLogger = BatchingProgressLogger.create(log, requestCorrelationId, baseTask, concurrency);

        return new TaskProgressLogger(batchingProgressLogger, baseTask, leafTaskVisitor);
    }

    @Override
    public void logProgress(long progress) {
        batchingProgressLogger.logProgress(progress, () -> null);
    }

    @Override
    public long reset(long newTaskVolume) {
        return batchingProgressLogger.reset(newTaskVolume);
    }

    @Override
    public void release() {
        batchingProgressLogger.release();
    }

    public void logBeginSubTask(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);
        if (parentTask == null) {
            logStart(taskName);
        } else {
            startSubTask(taskName);
        }
        reset(task.getProgress().volume());
    }

    public void logEndSubTask(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);
        log100OnLeafTaskFinish(task);
        if (parentTask == null) {
            logFinish(taskName);
        } else {
            finishSubTask(taskName);
        }
    }

    public void logEndSubTaskWithFailure(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);

        log100OnLeafTaskFinish(task);
        if (parentTask == null) {
            logFinishWithFailure(taskName);
        } else {
            logFinishSubtaskWithFailure(taskName);
        }
    }

    void startSubTask(String subTaskName) {
        setTask(getTask() + TASK_SEPARATOR + subTaskName);
        logStart();
    }

    void finishSubTask(String subTaskName) {
        logFinish();
        var endIndex = getTask().lastIndexOf(TASK_SEPARATOR + subTaskName);
        if (endIndex == -1) {
            throw new IllegalArgumentException("Unknown subtask: " + subTaskName);
        }
        var task = getTask().substring(0, endIndex);
        setTask(task);
    }

    private String getTask() {
        return batchingProgressLogger.getTask();
    }

    private void setTask(String task) {
        batchingProgressLogger.setTask(task);
    }

    private void logMessage(Supplier<String> msg) {
        batchingProgressLogger.logMessage(msg);
    }

    private void logMessage(String msg) {
        logMessage(() -> msg);
    }


    private void logStart() {
        logStart("");
    }

    private void logStart(String message) {
        logMessage((message + TASK_SEPARATOR + "Start").trim());
    }

    private void logFinish() {
        logFinish("");
    }

    private void logFinish(String message) {
        logMessage((message + TASK_SEPARATOR + "Finished").trim());
    }

    private void logFinishWithFailure() {
        logFinishWithFailure("");
    }

    private void logFinishWithFailure(String message) {
        logMessage((message + TASK_SEPARATOR + "Failed").trim());
    }

    private String boundedIterationsTaskName(
        IterativeTask iterativeTask,
        Task task
    ) {
        var maxIterations = iterativeTask.maxIterations();
        var currentIteration = iterativeTask.currentIteration() + 1;

        return formatWithLocale(
            "%s %d of %d",
            taskDescription(task),
            currentIteration,
            maxIterations
        );
    }

    private String unboundedIterationsTaskName(
        IterativeTask iterativeTask,
        Task task
    ) {
        var currentIteration = iterativeTask.currentIteration() + 1;

        return formatWithLocale(
            "%s %d",
            taskDescription(task),
            currentIteration
        );
    }

    private String taskDescription(Task task, Task parentTask) {
        String taskName;
        if (parentTask instanceof IterativeTask iterativeParentTask) {
            var iterativeTaskMode = iterativeParentTask.mode();

            taskName = switch (iterativeTaskMode) {
                case DYNAMIC, FIXED -> boundedIterationsTaskName(iterativeParentTask, task);
                case OPEN -> unboundedIterationsTaskName(iterativeParentTask, task);
            };
        } else {
            taskName = taskDescription(task);
        }
        return taskName;
    }

    private String taskDescription(Task nextTask) {
        return nextTask == baseTask
            ? ""
            : nextTask.description();
    }

    private void log100OnLeafTaskFinish(Task task) {
        task.visit(loggingLeafTaskVisitor);
    }

    private void logFinishSubtaskWithFailure(String subTaskName) {
        logFinishWithFailure();
        var endIndex = getTask().indexOf(TASK_SEPARATOR + subTaskName);
        if (endIndex == -1) {
            throw new IllegalArgumentException("Unknown subtask: " + subTaskName);
        }
        var task = getTask().substring(0, endIndex);
        setTask(task);
    }
}
