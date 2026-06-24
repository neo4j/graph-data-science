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
package org.neo4j.gds.progress.tracking;

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.utilities.BatchSizeCalculator;

final class BatchingTaskProgressTrackerWithLogging extends ProgressTrackerAdapter implements BatchingTaskProgressTracker {
    private static final BatchSizeCalculator BatchSizeCalculator = new BatchSizeCalculator();

    private final long batchSize;

    private long rowCounter = 0;

    private BatchingTaskProgressTrackerWithLogging(ProgressTracker delegate, long batchSize) {
        super(delegate);
        this.batchSize = batchSize;
    }

    static BatchingTaskProgressTrackerWithLogging create(
        ProgressTracker delegate,
        long volume,
        Concurrency concurrency
    ) {
        var batchSize = BatchSizeCalculator.calculateBatchSize(volume, concurrency);

        return new BatchingTaskProgressTrackerWithLogging(delegate, batchSize);
    }

    @Override
    public void onProgress() {
        if (++rowCounter == batchSize) {
            super.onProgress(batchSize);
            rowCounter = 0;
        }
    }
}
