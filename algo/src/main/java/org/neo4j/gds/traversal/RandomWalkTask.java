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
package org.neo4j.gds.traversal;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

final class RandomWalkTask implements Runnable {

    private final Graph graph;
    private final NextNodeSupplier nextNodeSupplier;
    private final BlockingQueue<long[]> walks;
    private final int walksPerNode;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final RandomWalkSampler sampler;
    private final long[][] buffer;

    private int bufferLength;

    RandomWalkTask(
        Graph graph,
        NextNodeSupplier nextNodeSupplier,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        BlockingQueue<long[]> walks,
        int walksPerNode,
        int walkLength,
        double returnFactor,
        double inOutFactor,
        long randomSeed,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.graph = graph;
        this.nextNodeSupplier = nextNodeSupplier;
        this.walks = walks;
        this.walksPerNode = walksPerNode;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;

        this.buffer = new long[1000][];
        this.sampler = RandomWalkSampler.create(
            graph,
            cumulativeWeightSupplier,
            walkLength,
            returnFactor,
            inOutFactor,
            randomSeed
        );
    }

    @Override
    public void run() {
        long nodeId;

        while (true) {
            nodeId = nextNodeSupplier.nextNode();

            if (nodeId == NextNodeSupplier.NO_MORE_NODES) break;

            if (graph.degree(nodeId) == 0) {
                progressTracker.logProgress();
                continue;
            }
            sampler.prepareForNewNode(nodeId);

            for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                var path = sampler.walk(nodeId);
                boolean shouldContinue = consumePath(path);
                if (!shouldContinue) {
                    break;
                }
            }
            progressTracker.logProgress();
        }
        flushBuffer(bufferLength);
    }

    private boolean consumePath(long[] path) {
        buffer[bufferLength++] = path;

        if (bufferLength == buffer.length) {
            var shouldStop = flushBuffer(bufferLength);
            bufferLength = 0;
            return shouldStop;
        }
        return true;
    }

    // returns false if execution should be stopped, otherwise true
    private boolean flushBuffer(int bufferLength) {
        bufferLength = Math.min(bufferLength, this.buffer.length);

        int i = 0;
        while (i < bufferLength && terminationFlag.running()) {
            try {
                // allow termination to occur if queue is full
                if (walks.offer(this.buffer[i], 100, TimeUnit.MILLISECONDS)) {
                    i++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return terminationFlag.running();
    }
}
