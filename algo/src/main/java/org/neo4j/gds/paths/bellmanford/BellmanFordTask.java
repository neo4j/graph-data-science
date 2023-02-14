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
import org.neo4j.gds.paths.delta.TentativeDistances;

import java.util.concurrent.atomic.AtomicLong;

public class BellmanFordTask implements Runnable {

    private static final long LOCAL_QUEUE_BOUND = 1000;

    private final Graph localGraph;
    private final TentativeDistances distances;

    private final HugeLongArray frontier;
    private final AtomicLong frontierIndex;
    private final AtomicLong frontierSize;
    private long localQueueIndex;

    private final HugeLongArray localQueue;
    private final HugeAtomicBitSet insideQueue;
    private BellmanFordPhase phase;

    BellmanFordTask(
        Graph localGraph,
        TentativeDistances distances,
        HugeLongArray frontier,
        AtomicLong frontierIndex,
        AtomicLong frontierSize,
        HugeAtomicBitSet insideQueue
    ) {
        this.localGraph = localGraph;
        this.distances = distances;
        this.frontier = frontier;
        this.frontierIndex = frontierIndex;
        this.localQueue = HugeLongArray.newArray(localGraph.nodeCount());
        this.insideQueue = insideQueue;
        this.frontierSize = frontierSize;
        this.phase = BellmanFordPhase.RUN;
    }

    private void processNode(long nodeId) {
        insideQueue.clear(nodeId);
        localGraph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
            tryToUpdate(s, t, w);
            return true;
        });
    }

    private void tryToUpdate(long sourceNodeId, long targetNodeId, double weight) {
        var oldDist = distances.distance(targetNodeId);
        var newDist = distances.distance(sourceNodeId) + weight;
        while (Double.compare(newDist, oldDist) < 0) {
            var witness = distances.compareAndExchange(targetNodeId, oldDist, newDist, sourceNodeId);

            if (Double.compare(witness, oldDist) == 0) {
                if (!insideQueue.getAndSet(targetNodeId)) {
                    localQueue.set(localQueueIndex++, targetNodeId);
                }
                break;

            }
            // CAX failed, retry
            oldDist = witness;
        }
    }


    private void relaxPhase() {
        long offset;
        while ((offset = frontierIndex.getAndAdd(64)) < frontierSize.get()) {
            var chunkSize = Math.min(offset + 64, frontierSize.get());
            for (long idx = offset; idx < chunkSize; idx++) {
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
        } else {
            sync();
            this.phase = BellmanFordPhase.RUN;
        }
    }

    enum BellmanFordPhase {
        RUN, SYNC
    }


}
