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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;
import org.neo4j.gds.traversal.NextNodeSupplier;

import java.util.concurrent.atomic.AtomicLong;

final class Node2VecRandomWalkTask implements Runnable {

    private final Graph graph;
    private final NextNodeSupplier nextNodeSupplier;
    private final int walksPerNode;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final AtomicLong walkIndex;
    private final CompressedRandomWalks compressedRandomWalks;
    private final RandomWalkProbabilities.Builder randomWalkProbabilitiesBuilder;
    private final RandomWalkSampler sampler;
    private final int walkBufferSize;
    private int walks;
    private int maxWalkLength;
    private long maxIndex;

    Node2VecRandomWalkTask(
        Graph graph,
        NextNodeSupplier nextNodeSupplier,
        int walksPerNode,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        AtomicLong walkIndex,
        CompressedRandomWalks compressedRandomWalks,
        RandomWalkProbabilities.Builder randomWalkProbabilitiesBuilder,
        int walkBufferSize,
        long randomSeed,
        int walkLength,
        double returnFactor,
        double inOutFactor
    ) {
        this.graph = graph;
        this.nextNodeSupplier = nextNodeSupplier;
        this.walksPerNode = walksPerNode;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.walkIndex = walkIndex;
        this.compressedRandomWalks = compressedRandomWalks;
        this.randomWalkProbabilitiesBuilder = randomWalkProbabilitiesBuilder;
        this.walkBufferSize = walkBufferSize;

        this.sampler = RandomWalkSampler.create(
            graph,
            cumulativeWeightSupplier,
            walkLength,
            returnFactor,
            inOutFactor,
            randomSeed
        );
        this.walks = 0;
        this.maxWalkLength = 0;
        this.maxIndex = 0;
    }

    private boolean consumePath(long[] path) {
        var index = walkIndex.getAndIncrement(); //perhaps we can also use a buffer to minimize walkIndex atomic operations
        maxIndex = index;
        randomWalkProbabilitiesBuilder.registerWalk(path);
        compressedRandomWalks.add(index, path);
        maxWalkLength = Math.max(path.length, maxWalkLength);
        if (walks++ == walkBufferSize) {
            walks = 0;
            return this.terminationFlag.running();
        }
        return true;
    }

    int maxWalkLength() {
        return maxWalkLength;
    }

    long maxIndex() {
        return maxIndex;
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
    }
}
