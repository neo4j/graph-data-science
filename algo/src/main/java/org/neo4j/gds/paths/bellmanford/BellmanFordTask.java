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
package org.neo4j.gds.paths.bellmanford;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BellmanFordTask implements Runnable {

    private static final long LOCAL_QUEUE_BOUND = 1000;

    private final Graph localGraph;
    private final DistanceTracker distances;

    private final HugeLongArray frontier;
    private final AtomicLong frontierIndex;
    private final AtomicLong frontierSize;
    private long localQueueIndex;
    private final long lengthBound;

    private final HugeLongArray localQueue;
    private final HugeAtomicBitSet validBitset;
    private BellmanFordPhase phase;
    private boolean locallyFoundNegativeCycle = false;
    private final AtomicBoolean negativeCycleFound;

    BellmanFordTask(
        Graph localGraph,
        DistanceTracker distances,
        HugeLongArray frontier,
        AtomicLong frontierIndex,
        AtomicLong frontierSize,
        HugeAtomicBitSet validBitset,
        AtomicBoolean negativeCycleFound
    ) {
        this.lengthBound = localGraph.nodeCount();
        this.localGraph = localGraph;
        this.distances = distances;
        this.frontier = frontier;
        this.frontierIndex = frontierIndex;
        this.localQueue = HugeLongArray.newArray(localGraph.nodeCount());
        this.validBitset = validBitset;
        this.frontierSize = frontierSize;
        this.phase = BellmanFordPhase.RUN;
        this.negativeCycleFound = negativeCycleFound;
    }

    private void processNode(long nodeId) {
        validBitset.clear(nodeId);
        if (distances.length(nodeId) >= lengthBound) {
            locallyFoundNegativeCycle = true;
            negativeCycleFound.set(true);
            return;
        }
        localGraph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
            tryToUpdate(s, t, w);
            return true;
        });
    }

    private void tryToUpdate(long sourceNodeId, long targetNodeId, double weight) {
        var oldDist = distances.distance(targetNodeId);
        var newDist = distances.distance(sourceNodeId) + weight;
        var newLength = distances.length(sourceNodeId) + 1;
        while (Double.compare(newDist, oldDist) < 0) {
            var witness = distances.compareAndExchange(targetNodeId, oldDist, newDist, sourceNodeId, newLength);

            if (Double.compare(witness, oldDist) == 0) {
                if (!validBitset.getAndSet(targetNodeId)) {
                    localQueue.set(localQueueIndex++, targetNodeId);
                }
                break;

            }
            // CAX failed, retry
            oldDist = distances.distance(targetNodeId);
        }
    }

    private void relaxPhase() {
        long offset;
        while ((offset = frontierIndex.getAndAdd(64)) < frontierSize.get()) {
            if (negativeCycleFound.get()) {
                break;
            }
            var chunkSize = Math.min(offset + 64, frontierSize.get());
            for (long idx = offset; (idx < chunkSize && !locallyFoundNegativeCycle); idx++) {
                long nodeId = frontier.get(idx);
                processNode(nodeId);
            }
        }
        //do some local processing if the localQueue is small enough
        while (localQueueIndex > 0 && localQueueIndex < LOCAL_QUEUE_BOUND) {
            long nodeId = localQueue.get(--localQueueIndex);
            processNode(nodeId);
        }
    }

    private void sync() {
        long currentIndex = frontierSize.getAndAdd(localQueueIndex);

        for (long u = 0; u < localQueueIndex; ++u) {
            frontier.set(currentIndex++, localQueue.get(u));
        }
    }

    @Override
    public void run() {
        if (phase == BellmanFordPhase.RUN) {
            localQueueIndex = 0;
            relaxPhase();
            phase = BellmanFordPhase.SYNC;
        } else if (phase == BellmanFordPhase.SYNC) {
            sync();
            this.phase = BellmanFordPhase.RUN;
        }
    }

    enum BellmanFordPhase {
        RUN, SYNC
    }
}
