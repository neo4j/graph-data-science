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
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.BatchingProgressLogger;
import org.neo4j.gds.core.utils.progress.ProgressLogger;

import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@SuppressWarnings("ClassCanBeRecord")
public final class TaskProgressLogger implements ProgressLogger {
    private final BatchingProgressLogger batchingProgressLogger;
    private final Task baseTask;
    private final TaskVisitor loggingLeafTaskVisitor;

    static TaskProgressLogger create(LoggerForProgressTracking log, RequestCorrelationId requestCorrelationId, Task baseTask, Concurrency concurrency) {
        var batchingProgressLogger = new BatchingProgressLogger(log, requestCorrelationId, baseTask, concurrency);
        var loggingLeafTaskVisitor = new LoggingLeafTaskVisitor(batchingProgressLogger);

        return new TaskProgressLogger(batchingProgressLogger, baseTask, loggingLeafTaskVisitor);
    }

    static TaskProgressLogger create(LoggerForProgressTracking log, RequestCorrelationId requestCorrelationId, Task baseTask, Concurrency concurrency, TaskVisitor leafTaskVisitor) {
        var batchingProgressLogger = new BatchingProgressLogger(log, requestCorrelationId, baseTask, concurrency);

        return new TaskProgressLogger(batchingProgressLogger, baseTask, leafTaskVisitor);
    }

    private TaskProgressLogger(BatchingProgressLogger batchingProgressLogger, Task baseTask, TaskVisitor loggingLeafTaskVisitor) {
        this.batchingProgressLogger = batchingProgressLogger;
        this.baseTask = baseTask;
        this.loggingLeafTaskVisitor = loggingLeafTaskVisitor;
    }

    void logBeginSubTask(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);
        if (parentTask == null) {
            logStart(taskName);
        } else {
            startSubTask(taskName);
        }
        reset(task.getProgress().volume());
    }

    void logEndSubTask(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);
        log100OnLeafTaskFinish(task);
        if (parentTask == null) {
            logFinish(taskName);
        } else {
            finishSubTask(taskName);
        }
    }

    void logEndSubTaskWithFailure(Task task, @Nullable Task parentTask) {
        var taskName = taskDescription(task, parentTask);

        log100OnLeafTaskFinish(task);
        if (parentTask == null) {
            logFinishWithFailure(taskName);
        } else {
            logFinishSubtaskWithFailure(taskName);
        }
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

    @Override
    public String getTask() {
        return batchingProgressLogger.getTask();
    }

    @Override
    public void setTask(String task) {
        batchingProgressLogger.setTask(task);
    }

    @Override
    public void logProgress(Supplier<String> msgFactory) {
        batchingProgressLogger.logProgress(msgFactory);
    }

    @Override
    public void logProgress(long progress, Supplier<String> msgFactory) {
        batchingProgressLogger.logProgress(progress, msgFactory);
    }

    @Override
    public void logMessage(Supplier<String> msg) {
        batchingProgressLogger.logMessage(msg);
    }

    @Override
    public void logFinishPercentage() {
        batchingProgressLogger.logFinishPercentage();
    }

    @Override
    public void logDebug(Supplier<String> msg) {
        batchingProgressLogger.logDebug(msg);
    }

    @Override
    public void logWarning(String msg) {
        batchingProgressLogger.logWarning(msg);
    }

    @Override
    public void logError(String msg) {
        batchingProgressLogger.logError(msg);
    }

    @Override
    public long reset(long newTaskVolume) {
        return batchingProgressLogger.reset(newTaskVolume);
    }

    @Override
    public void release() {
        batchingProgressLogger.release();
    }
}
