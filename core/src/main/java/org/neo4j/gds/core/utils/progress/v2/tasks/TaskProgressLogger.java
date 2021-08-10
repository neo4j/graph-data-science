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

import java.util.function.Supplier;

public class TaskProgressLogger implements ProgressLogger {

    private final ProgressLogger progressLogger;

    public TaskProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
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
}
