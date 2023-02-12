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
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.logging.Log;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class TaskProgressLogger extends BatchingProgressLogger {

    private final Task baseTask;
    private final TaskVisitor loggingLeafTaskVisitor;

    TaskProgressLogger(Log log, Task baseTask, int concurrency) {
        super(log, baseTask, concurrency);
        this.baseTask = baseTask;
        this.loggingLeafTaskVisitor = new LoggingLeafTaskVisitor(this);

    }
    TaskProgressLogger(Log log, Task baseTask, int concurrency, TaskVisitor leafTaskVisitor) {
        super(log, baseTask, concurrency);
        this.baseTask = baseTask;
        this.loggingLeafTaskVisitor = leafTaskVisitor;
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

    private void log100OnLeafTaskFinish(Task task) {
        task.visit(loggingLeafTaskVisitor);
    }

    private static final class LoggingLeafTaskVisitor implements TaskVisitor {

        private final ProgressLogger progressLogger;

        private LoggingLeafTaskVisitor(ProgressLogger progressLogger) {
            this.progressLogger = progressLogger;
        }

        @Override
        public void visitLeafTask(LeafTask leafTask) {
            progressLogger.logFinishPercentage();
        }
    }
}
