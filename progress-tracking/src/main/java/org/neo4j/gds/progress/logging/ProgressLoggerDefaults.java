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

import java.util.function.Supplier;

/**
 * A means to yanking out implementation details from the interface.
 * Facilitates internal reuse without exposing stuff outside module.
 */
abstract class ProgressLoggerDefaults implements ProgressLogger {
    String TASK_SEPARATOR = " :: ";

    abstract String getTask();

    abstract void setTask(String task);

    abstract void logMessage(Supplier<String> msg);

    void logMessage(String msg) {
        logMessage(() -> msg);
    }

    final void logStart() {
        logStart("");
    }

    final void logStart(String message) {
        logMessage((message + TASK_SEPARATOR + "Start").trim());
    }

    final void logFinish() {
        logFinish("");
    }

    final void logFinish(String message) {
        logMessage((message + TASK_SEPARATOR + "Finished").trim());
    }

    final void logFinishWithFailure() {
        logFinishWithFailure("");
    }

    final void logFinishWithFailure(String message) {
        logMessage((message + TASK_SEPARATOR + "Failed").trim());
    }

    public final void startSubTask(String subTaskName) {
        setTask(getTask() + TASK_SEPARATOR + subTaskName);
        logStart();
    }

    public final void finishSubTask(String subTaskName) {
        logFinish();
        var endIndex = getTask().lastIndexOf(TASK_SEPARATOR + subTaskName);
        if (endIndex == -1) {
            throw new IllegalArgumentException("Unknown subtask: " + subTaskName);
        }
        var task = getTask().substring(0, endIndex);
        setTask(task);
    }
}
