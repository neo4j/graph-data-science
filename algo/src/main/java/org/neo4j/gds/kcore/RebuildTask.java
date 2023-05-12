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
package org.neo4j.gds.kcore;

import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.kcore.KCoreDecomposition.UNASSIGNED;

public class RebuildTask implements Runnable {

    private final Partition partition;
    private final AtomicLong atomicIndex;
    private final HugeIntArray core;
    private final HugeLongArray nodeOrder;
    private final HugeLongArrayQueue localQueue;

    public static long memoryEstimation(long nodeCount) {

        return HugeLongArrayQueue.memoryEstimation(nodeCount);
    }

    RebuildTask(Partition partition, AtomicLong atomicIndex, HugeIntArray core, HugeLongArray nodeOrder) {
        this.partition = partition;
        this.atomicIndex = atomicIndex;
        this.core = core;
        this.nodeOrder = nodeOrder;
        localQueue = HugeLongArrayQueue.newQueue(nodeOrder.size());
    }

    @Override
    public void run() {
        long startNode = partition.startNode();
        long endNode = startNode + partition.nodeCount();
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            if (core.get(nodeId) == UNASSIGNED) {
                localQueue.add(nodeId);
            }
        }
        long localQueueSize = localQueue.size();
        long currentIndex = atomicIndex.getAndAdd(localQueueSize);
        while (!localQueue.isEmpty()) {
            long nodeId = localQueue.remove();
            nodeOrder.set(currentIndex++, nodeId);
        }
    }
}
