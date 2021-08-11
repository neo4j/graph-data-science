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

import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.logging.Log;

import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TaskProgressLogger implements ProgressLogger {

    private final ProgressLogger progressLogger;
    private final Task baseTask;

    public TaskProgressLogger(ProgressLogger progressLogger, Task baseTask) {
        this.progressLogger = progressLogger;
        this.baseTask = baseTask;
    }

    void logBeginSubTask(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);
        if (parentTask == null) {
            progressLogger.logStart(taskName);
        } else {
            progressLogger.startSubTask(taskName);
        }
        progressLogger.reset(task.getProgress().volume());
    }

    void logEndSubTask(Task task, Task parentTask) {
        var taskName = taskDescription(task, parentTask);
        if (parentTask == null) {
            progressLogger.logFinish(taskName);
        } else {
            progressLogger.finishSubTask(taskName);
        }
    }

    ProgressLogger internalProgressLogger() {
        return this.progressLogger;
    }

    @Override
    public String getTask() {
        return progressLogger.getTask();
    }

    @Override
    public void setTask(String task) {
        progressLogger.setTask(task);
    }

    @Override
    public void logProgress(Supplier<String> msgFactory) {
        progressLogger.logProgress(msgFactory);
    }

    @Override
    public void logProgress(long progress, Supplier<String> msgFactory) {
        progressLogger.logProgress(progress, msgFactory);
    }

    @Override
    public void logMessage(Supplier<String> msg) {
        progressLogger.logMessage(msg);
    }

    @Override
    public long reset(long newTaskVolume) {
        return progressLogger.reset(newTaskVolume);
    }

    @Override
    public void release() {
        progressLogger.release();
    }

    @Override
    public Log getLog() {
        return progressLogger.getLog();
    }

    @Override
    public void logProgress(double percentDone, Supplier<String> msg) {
        progressLogger.logProgress(percentDone, msg);
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
        if (parentTask instanceof IterativeTask) {
            var iterativeParentTask = (IterativeTask) parentTask;
            var iterativeTaskMode = iterativeParentTask.mode();
            switch (iterativeTaskMode) {
                case DYNAMIC:
                case FIXED:
                    taskName = boundedIterationsTaskName(iterativeParentTask, task);
                    break;
                case OPEN:
                    taskName = unboundedIterationsTaskName(iterativeParentTask, task);
                    break;
                default:
                    throw new UnsupportedOperationException(formatWithLocale("Enum value %s is not supported", iterativeTaskMode));
            }
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
}
