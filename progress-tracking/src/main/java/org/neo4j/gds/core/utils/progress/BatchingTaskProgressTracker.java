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
package org.neo4j.gds.core.utils.progress;

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTrackerAdapter;
import org.neo4j.gds.core.utils.progress.tasks.Task;

public final class BatchingTaskProgressTracker {

    private BatchingTaskProgressTracker() {}

    public static ProgressTracker create(ProgressTracker delegate, long volume, Concurrency concurrency) {
        return volume == Task.UNKNOWN_VOLUME
            ? new WithoutLogging(delegate)
            : new WithLogging(delegate, volume, concurrency);
    }

    static class WithLogging extends ProgressTrackerAdapter {

        private final long batchSize;
        private long rowCounter;

        WithLogging(ProgressTracker delegate, long volume, Concurrency concurrency) {
            super(delegate);
            this.batchSize = BatchingProgressLogger.calculateBatchSize(volume, concurrency);
            this.rowCounter = 0;
        }

        @Override
        public void logProgress() {
            if (++rowCounter == batchSize) {
                super.logProgress(batchSize);
                rowCounter = 0;
            }
        }
    }

    static class WithoutLogging extends ProgressTrackerAdapter {

        WithoutLogging(ProgressTracker delegate) {
            super(delegate);
        }

        @Override
        public void logProgress() {
        }

        @Override
        public void logProgress(long value) {
        }

        @Override
        public void logProgress(long value, String messageTemplate) {
        }
    }
}
