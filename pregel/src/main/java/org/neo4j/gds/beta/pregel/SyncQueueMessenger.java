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
package org.neo4j.gds.beta.pregel;

import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

public class SyncQueueMessenger implements Messenger<PrimitiveSyncDoubleQueues.Iterator> {

    private final PrimitiveSyncDoubleQueues queues;

    SyncQueueMessenger(long nodeCount, AllocationTracker allocationTracker) {
        this.queues = PrimitiveSyncDoubleQueues.of(nodeCount, allocationTracker);
    }

    static MemoryEstimation memoryEstimation() {
        return PrimitiveSyncDoubleQueues.memoryEstimation();
    }

    @Override
    public void initIteration(int iteration) {
        queues.swapQueues();
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        queues.push(targetNodeId, message);
    }

    @Override
    public PrimitiveSyncDoubleQueues.Iterator messageIterator() {
        return new PrimitiveSyncDoubleQueues.Iterator();
    }

    @Override
    public void initMessageIterator(PrimitiveSyncDoubleQueues.Iterator messageIterator, long nodeId, boolean isFirstIteration) {
        queues.initIterator(messageIterator, nodeId);
    }

    @Override
    public void release() {
        queues.release();
    }
}
