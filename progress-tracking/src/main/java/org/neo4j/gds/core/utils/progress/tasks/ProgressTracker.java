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

import java.util.function.Function;

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
        public void onProgress(long value) {
        }

        @Override
        public void onProgress(Function<Long, Long> valueCalculator) {

        }

        @Override
        public void onProgress(long value, String messageTemplate) {

        }

        @Override
        public void setVolume(long volume) {
        }

        @Override
        public void setSteps(long steps) {

        }

        @Override
        public void logSteps(long steps) {

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

    void onProgress(long value);

    void onProgress(Function<Long, Long> valueCalculator);

    default void onProgress() {
        onProgress(1);
    }

    void onProgress(long value, String messageTemplate);

    // prefer setting volume via factory method for leaves
    // to make root progress available from the start
    void setVolume(long volume);

    void release();

    void setSteps(long steps);

    void logSteps(long steps);
}
