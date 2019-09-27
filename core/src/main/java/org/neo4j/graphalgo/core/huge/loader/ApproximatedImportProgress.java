/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.atomic.AtomicLong;

public final class ApproximatedImportProgress implements ImportProgress {

    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final AtomicLong progress;
    private final long mask;
    private final long operations;

    public ApproximatedImportProgress(
            ProgressLogger progressLogger,
            AllocationTracker tracker,
            long expectedNodeOperations,
            long expectedRelationshipOperations) {
        this.progressLogger = progressLogger;
        this.tracker = tracker;
        operations = expectedNodeOperations + expectedRelationshipOperations;
        mask = (BitUtil.nearbyPowerOfTwo(operations) >>> 6) - 1L;
        progress = new AtomicLong();
    }

    @Override
    public void singleNodeImported() {
        long ops = progress.incrementAndGet();
        if ((ops & mask) == 0L) {
            progressLogger.logProgress(ops, operations, tracker);
        }
    }

    @Override
    public void nodesImported(final int numImported) {
        trackProgress(numImported);
    }

    @Override
    public void relationshipsImported(int numImported) {
        trackProgress(numImported);
    }

    private void trackProgress(final int numImported) {
        long opsBefore = progress.getAndAdd(numImported);
        long opsAfter = opsBefore + numImported;
        if ((opsAfter & mask) < (opsBefore & mask)) {
            progressLogger.logProgress(opsAfter, operations, tracker);
        }
    }
}
