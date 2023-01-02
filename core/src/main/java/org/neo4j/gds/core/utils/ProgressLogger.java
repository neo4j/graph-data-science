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
package org.neo4j.gds.core.utils;

import java.util.function.Supplier;

public interface ProgressLogger {

    String TASK_SEPARATOR = " :: ";

    Supplier<String> NO_MESSAGE = () -> null;

    String getTask();

    void setTask(String task);

    default void logProgress() {
        logProgress(NO_MESSAGE);
    }

    void logProgress(Supplier<String> msgFactory);

    default void logProgress(long progress) {
        logProgress(progress, NO_MESSAGE);
    }

    void logProgress(long progress, Supplier<String> msgFactory);

    void logMessage(Supplier<String> msg);

    void logFinishPercentage();

    default void logMessage(String msg) {
        logMessage(() -> msg);
    }

    void logDebug(String msg);

    void logWarning(String msg);

    void logError(String msg);

    default void logStart() {
        logStart("");
    }

    default void logStart(String message) {
        logMessage((message + TASK_SEPARATOR + "Start").trim());
    }

    default void logFinish() {
        logFinish("");
    }

    default ProgressLogger logFinish(String message) {
        logMessage((message + TASK_SEPARATOR + "Finished").trim());
        return this;
    }

    default ProgressLogger logFinishWithFailure() {
        return logFinishWithFailure("");
    }

    default ProgressLogger logFinishWithFailure(String message) {
        logMessage((message + TASK_SEPARATOR + "Failed").trim());
        return this;
    }

    default ProgressLogger logFinishSubtaskWithFailure(String subTaskName) {
        logFinishWithFailure();
        var endIndex = getTask().indexOf(TASK_SEPARATOR + subTaskName);
        if (endIndex == -1) {
            throw new IllegalArgumentException("Unknown subtask: " + subTaskName);
        }
        var task = getTask().substring(0, endIndex);
        setTask(task);
        return this;
    }

    default ProgressLogger startSubTask(String subTaskName) {
        setTask(getTask() + TASK_SEPARATOR + subTaskName);
        logStart();
        return this;
    }

    default ProgressLogger finishSubTask(String subTaskName) {
        logFinish();
        var endIndex = getTask().indexOf(TASK_SEPARATOR + subTaskName);
        if (endIndex == -1) {
            throw new IllegalArgumentException("Unknown subtask: " + subTaskName);
        }
        var task = getTask().substring(0, endIndex);
        setTask(task);
        return this;
    }

    long reset(long newTaskVolume);

    void release();
}
