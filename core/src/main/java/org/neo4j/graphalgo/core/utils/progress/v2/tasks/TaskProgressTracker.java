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
package org.neo4j.graphalgo.core.utils.progress.v2.tasks;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.core.utils.ProgressLogger;

import java.util.Stack;

public class TaskProgressTracker implements ProgressTracker {

    private final Task baseTask;
    private final ProgressLogger progressLogger;
    private final Stack<Task> nestedTasks;
    private Task currentTask;

    public TaskProgressTracker(
        Task baseTask,
        ProgressLogger progressLogger
    ) {
        this.baseTask = baseTask;
        this.progressLogger = progressLogger;
        this.currentTask = null;
        this.nestedTasks = new Stack<>();
    }

    @Override
    public void beginSubTask() {
        if (currentTask == null) {
            currentTask = baseTask;
        } else {
            nestedTasks.add(currentTask);
            currentTask = currentTask.nextSubtask();
        }
        currentTask.start();
        progressLogger.logStart(currentTask.description());
        progressLogger.reset(currentTask.getProgress().volume());
    }

    @Override
    public void endSubTask() {
        if (currentTask == null) {
            throw new IllegalStateException("No more running tasks");
        }
        currentTask.finish();
        progressLogger.logFinish(currentTask.description());
        currentTask = nestedTasks.isEmpty()
            ? null
            : nestedTasks.pop();
    }

    @Override
    public void logProgress() {
        logProgress(1);
    }

    @Override
    public void logProgress(long value) {
        currentTask.logProgress(value);
        progressLogger.logProgress(value);
    }

    @Override
    public void setVolume(long volume) {
        currentTask.setVolume(volume);
        progressLogger.reset(volume);
    }

    @Override
    public ProgressLogger progressLogger() {
        return progressLogger;
    }

    @Override
    public void release() {
    }

    @TestOnly
    Task currentSubTask() {
        return currentTask;
    }
}
