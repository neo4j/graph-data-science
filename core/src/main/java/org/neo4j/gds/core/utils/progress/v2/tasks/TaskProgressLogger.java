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
package org.neo4j.gds.core.utils.progress.v2.tasks;

import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.logging.Log;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TaskProgressLogger implements ProgressLogger {

    private final ProgressLogger progressLogger;
    private final Task baseTask;
    private final TaskVisitor loggingTaskStartVisitor;
    private final TaskVisitor loggingTaskEndVisitor;

    public TaskProgressLogger(ProgressLogger progressLogger, Task baseTask) {
        this.progressLogger = progressLogger;
        this.baseTask = baseTask;
        this.loggingTaskStartVisitor = new LoggingTaskVisitor(progressLogger::logStart);
        this.loggingTaskEndVisitor = new LoggingTaskVisitor(progressLogger::logFinish);
    }

    public void beginSubTask(Task task) {
        task.visit(loggingTaskStartVisitor);
        progressLogger.reset(task.getProgress().volume());
    }

    public void endSubTask(Task task) {
        task.visit(loggingTaskEndVisitor);
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

    private final class LoggingTaskVisitor implements TaskVisitor {

        private final Consumer<String> logMessageConsumer;

        private LoggingTaskVisitor(Consumer<String> logMessageConsumer) {
            this.logMessageConsumer = logMessageConsumer;
        }

        @Override
        public void visitLeafTask(LeafTask leafTask) {
            logMessageConsumer.accept(taskDescription(leafTask));
        }

        @Override
        public void visitIntermediateTask(Task task) {
            logMessageConsumer.accept(taskDescription(task));
        }

        @Override
        public void visitIterativeTask(IterativeTask iterativeTask) {
            var iterativeTaskMode = iterativeTask.mode();
            String iterationTaskName;
            switch (iterativeTaskMode) {
                case DYNAMIC:
                case FIXED:
                    iterationTaskName = boundedIterationsTaskName(iterativeTask);
                    break;
                case OPEN:
                    iterationTaskName = unboundedIterationsTaskName(iterativeTask);
                    break;
                default:
                    throw new UnsupportedOperationException(formatWithLocale("Enum value %s is not supported", iterativeTaskMode));
            }
            logMessageConsumer.accept(iterationTaskName);
        }

        private String boundedIterationsTaskName(IterativeTask iterativeTask) {
            var subTasksSize = iterativeTask.subTasks().size();
            var currentIteration = iterativeTask.currentIteration();

            return formatWithLocale(
                "%s %d of %d",
                taskDescription(iterativeTask),
                currentIteration,
                subTasksSize
            );
        }

        private String unboundedIterationsTaskName(IterativeTask iterativeTask) {
            var currentIteration = iterativeTask.currentIteration();

            return formatWithLocale(
                "%s %d",
                taskDescription(iterativeTask),
                currentIteration
            );
        }

        private String taskDescription(Task nextTask) {
            return nextTask == baseTask
                ? ""
                : nextTask.description();
        }
    }
}
