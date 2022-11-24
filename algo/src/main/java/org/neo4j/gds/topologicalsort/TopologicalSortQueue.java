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
package org.neo4j.gds.topologicalsort;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is how concurrency is implemented for the topological sort algorithm.
 *
 * It is actually a multi-queue: one different queue for each thread.
 *
 * Each thread only works on nodes with ids within its range. The ranges are continuous and even, in the hope that
 * sources spreading is not dependent on the node id, so that sources are evenly spread between the ranges.
 *
 * When a single queue is empty, it means its thread is currently idle.
 * When all the queues are empty, it means work is done. The queue will then return -1 for all fetch operations.
 *
 * To be able to identify when work is done, threads should only remove things from the queue after they finished
 * working on them (and potentially added other nodes to the queues in the process).
 */
class TopologicalSortQueue {
    private final int numThreads;

    // todo: maybe change to huge queues
    private final ArrayList<ConcurrentLinkedQueue<Long>> queues;

    private final long rangeSize;

    TopologicalSortQueue(long nodeCount, int numThreads) {
        queues = new ArrayList<>(numThreads);

        for(int i=0; i<numThreads; ++i) {
            queues.add(new ConcurrentLinkedQueue<>());
        }
        this.numThreads = numThreads;

        rangeSize = nodeCount / numThreads;
    }

    void add(long nodeId) {
        int index = (int) (nodeId / rangeSize);
        if(index == numThreads)
            --index;  // the last thread will get a little bit more nodes (up to numThreads - 1) - not a big deal
        queues.get(index).add(nodeId);
    }

    long peekBy(int threadId) {
        Long nodeId = queues.get(threadId).peek();
        if(nodeId == null) {
            if(isDone()) {
                finish();
            }
            // todo: a better parallelization scheme should be used before algorithm graduation
            while (nodeId == null) {
                nodeId = queues.get(threadId).peek();
            }
        }
        return nodeId;
    }

    void popBy(int threadId) {
        queues.get(threadId).poll();
    }

    private boolean isDone() {
        for(int i = 0; i < numThreads; ++i) {
            if(! queues.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void finish() {
        // adding -1 to all the queues will cause all threads to finish
        for(int i = 0; i < numThreads; ++i) {
            queues.get(i).add(-1L);
        }
    }
}
