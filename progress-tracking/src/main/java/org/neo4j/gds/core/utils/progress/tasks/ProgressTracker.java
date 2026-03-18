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

public interface ProgressTracker {
    ProgressTracker NULL_TRACKER = new ProgressTracker() {
        @Override
        public void setEstimatedResourceFootprint(MemoryRange memoryRangeInBytes) {
        }

        @Override
        public void requestedConcurrency(Concurrency concurrency) {

        }

        @Override
        public void beginSubTask() {
        }

        @Override
        public void beginSubTask(long taskVolume) {

        }

        @Override
        public void endSubTask() {
        }

        @Override
        public void beginSubTask(String expectedTaskDescription) {

        }

        @Override
        public void beginSubTask(String expectedTaskDescription, long taskVolume) {

        }

        @Override
        public void endSubTask(String expectedTaskDescription) {

        }

        @Override
        public void logProgress(long value) {
        }

        @Override
        public void logProgress(long value, String messageTemplate) {

        }

        @Override
        public void setVolume(long volume) {
        }

        @Override
        public long currentVolume() {
            return Task.UNKNOWN_VOLUME;
        }

        @Override
        public void setSteps(long steps) {

        }

        @Override
        public void logSteps(long steps) {

        }

        @Override
        public void logMessage(LogLevel level, String message) {
        }

        @Override
        public void logDebug(Supplier<String> messageSupplier) {
        }

        @Override
        public void release() {
        }

        @Override
        public void endSubTaskWithFailure() {

        }

        @Override
        public void endSubTaskWithFailure(String expectedTaskDescription) {

        }
    };

    void setEstimatedResourceFootprint(MemoryRange memoryEstimationInBytes);

    void requestedConcurrency(Concurrency concurrency);

    void beginSubTask();

    void beginSubTask(long taskVolume);

    void beginSubTask(String expectedTaskDescription);

    void beginSubTask(String expectedTaskDescription, long taskVolume);

    void endSubTask();

    void endSubTask(String expectedTaskDescription);

    void endSubTaskWithFailure();

    void endSubTaskWithFailure(String expectedTaskDescription);

    /**
     * @deprecated get rid or at least rename
     */
    @Deprecated
    void logProgress(long value);

    /**
     * @deprecated get rid or at least rename
     */
    @Deprecated
    default void logProgress() {
        logProgress(1);
    }

    /**
     * @deprecated get rid or at least rename
     */
    @Deprecated
    void logProgress(long value, String messageTemplate);

    // prefer setting volume via factory method for leaves
    // to make root progress available from the start
    void setVolume(long volume);

    /**
     * Returns the task volume of the currently running task or
     * {@link Task#UNKNOWN_VOLUME} if no task volume is set.
     */
    long currentVolume();

    /**
     * @deprecated Stop using progress tracker for logging
     */
    @Deprecated
    void logDebug(Supplier<String> messageSupplier);

    /**
     * @deprecated Stop using progress tracker for logging
     */
    @Deprecated
    default void logDebug(String message) {
        logMessage(LogLevel.DEBUG, message);
    }

    /**
     * @deprecated Stop using progress tracker for logging
     */
    @Deprecated
    default void logWarning(String message) {
        logMessage(LogLevel.WARNING, message);
    }

    /**
     * @deprecated Stop using progress tracker for logging
     */
    @Deprecated
    default void logInfo(String message) {
        logMessage(LogLevel.INFO, message);
    }

    /**
     * @deprecated Stop using progress tracker for logging
     */
    @Deprecated
    void logMessage(LogLevel level, String message);

    void release();

    void setSteps(long steps);

    void logSteps(long steps);
}
