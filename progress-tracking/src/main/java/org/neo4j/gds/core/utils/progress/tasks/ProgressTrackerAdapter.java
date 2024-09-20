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

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryRange;

import java.util.function.Supplier;

public abstract class ProgressTrackerAdapter implements ProgressTracker {

    private final ProgressTracker delegate;

    public ProgressTrackerAdapter(ProgressTracker delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setEstimatedResourceFootprint(MemoryRange memoryEstimationInBytes, Concurrency concurrency) {
        delegate.setEstimatedResourceFootprint(memoryEstimationInBytes, concurrency);
    }

    @Override
    public void beginSubTask() {
        delegate.beginSubTask();
    }

    @Override
    public void beginSubTask(long taskVolume) {
        delegate.beginSubTask(taskVolume);
    }

    @Override
    public void beginSubTask(String expectedTaskDescription) {
        delegate.beginSubTask(expectedTaskDescription);
    }

    @Override
    public void beginSubTask(String expectedTaskDescription, long taskVolume) {
        delegate.beginSubTask(expectedTaskDescription, taskVolume);
    }

    @Override
    public void endSubTask() {
        delegate.endSubTask();
    }

    @Override
    public void endSubTask(String expectedTaskDescription) {
        delegate.endSubTask(expectedTaskDescription);
    }

    @Override
    public void endSubTaskWithFailure() {
        delegate.endSubTaskWithFailure();
    }

    @Override
    public void endSubTaskWithFailure(String expectedTaskDescription) {
        delegate.endSubTaskWithFailure(expectedTaskDescription);
    }

    @Override
    public void logProgress(long value) {
        delegate.logProgress(value);
    }

    @Override
    public void logProgress(long value, String messageTemplate) {
        delegate.logProgress(value, messageTemplate);
    }

    @Override
    public void setVolume(long volume) {
        delegate.setVolume(volume);
    }

    @Override
    public long currentVolume() {
        return delegate.currentVolume();
    }

    @Override
    public void logDebug(Supplier<String> messageSupplier) {
        delegate.logDebug(messageSupplier);
    }

    @Override
    public void logMessage(LogLevel level, String message) {
        delegate.logMessage(level, message);
    }

    @Override
    public void release() {
        delegate.release();
    }

    @Override
    public void setSteps(long steps) {
        delegate.setSteps(steps);
    }

    @Override
    public void logSteps(long steps) {
        delegate.logSteps(steps);
    }
}
